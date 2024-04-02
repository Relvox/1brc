package main

import (
	"brc/pkg"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
)

var CHANS = 11

const (
	READ_BUF = 1024 * 1024 * 16

	BLOCK_CHAN_BUF = 64
	BATCH_CHAN_BUF = 6

	HKV_BATCH = 1024 * 1024

	MAP_SIZE = 41_343
)

type BlockChan = chan []byte
type Timings = pkg.Timings
type CityData = pkg.CityData
type HashKey = pkg.HashKey
type HKV = pkg.HKV
type Batch = []HKV
type HK = pkg.HK

type OutputMap = map[HashKey]*CityData

func main() {
	t := &pkg.Timings{Start: time.Now()}
	CHANS = runtime.NumCPU() - 1
	flagProf := flag.String("prof", "", "write cpu profile to file")
	flagTrace := flag.String("trace", "", "write trace to file")

	flagFile := flag.String("file", "../../data/measurements.txt", "1brc file")

	flagPercent := flag.Int("percent", 100, "% of file to process [0, 100]")
	flag.Parse()

	if *flagTrace != "" {
		f, _ := os.OpenFile(*flagTrace, os.O_CREATE|os.O_TRUNC, 0644)
		trace.Start(f)
		defer f.Close()
		defer trace.Stop()
	}
	if *flagProf != "" {
		f, err := os.Create(*flagProf)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	t.Since_Setup = time.Since(t.Start)

	chanChanBlock := ReadFile(*flagFile, *flagPercent, t)
	chanChanBatch := ParseBlocks(chanChanBlock, t)
	chanOutput := MapData(chanChanBatch, t)
	output := MergeMaps(chanOutput, t)
	PrintOutput(output, t)
	t.Report()
}

func ReadFile(file string, percent int, t *Timings) (chanChanBlock chan BlockChan) {
	tReadFile := time.Now()
	chanChanBlock = make(chan BlockChan, CHANS)
	chanBlocks := make([]BlockChan, CHANS)

	go func(t *Timings) {
		data, size, err := pkg.MMapFile(file)
		if err != nil {
			panic(err)
		}

		var limit int64 = int64(percent) * size / 100
		var chanIndex int
		for off := int64(0); off < limit; {
			buf := data[off:min(size, off+READ_BUF)]
			var n int
			for n = len(buf) - 1; n >= 0; n-- {
				if buf[n] == '\n' {
					break
				}
			}

			if n <= 0 {
				break
			}

			n++
			off += int64(n)
			t.SendBlocks = time.Now()
			if chanBlocks[chanIndex] == nil {
				chanBlocks[chanIndex] = make(BlockChan, BLOCK_CHAN_BUF)
				chanChanBlock <- chanBlocks[chanIndex]
			}

			chanBlocks[chanIndex] <- buf[:n]
			chanIndex = (chanIndex + 1) % CHANS
			t.Since_SendBlocks += time.Since(t.SendBlocks)
		}

		for i := range CHANS {
			if chanBlocks[i] == nil {
				chanBlocks[i] = make(BlockChan)
				chanChanBlock <- chanBlocks[chanIndex]
			}
			close(chanBlocks[i])
		}
		close(chanChanBlock)
		t.Since_ReadFile = time.Since(tReadFile)
	}(t)

	return chanChanBlock
}

func ParseBlocks(chanChanBlock chan BlockChan, t *Timings) (chanChanBatch chan chan Batch) {
	t.ParseBlocks = time.Now()
	chanChanBatch = make(chan chan Batch, CHANS)

	var wg sync.WaitGroup
	wg.Add(CHANS)
	go func(t *Timings) {
		for chanBlock := range chanChanBlock {
			go func(t *Timings) {
				chanBatch := make(chan Batch, BATCH_CHAN_BUF)
				chanChanBatch <- chanBatch
				batch := make(Batch, 0, HKV_BATCH)
				for block := range chanBlock {
					for i := 0; i < len(block); i++ {
						start := i
						var val int
						for ; block[i] != ';'; i++ {
						}
						key := block[start:i]
						i++
						val = 0
						sign := 1
						if block[i+1] == '-' {
							i++
							sign = -1
						}

						for ; block[i] != '.'; i++ {
							val += int(block[i]-'0') + val*10
						}
						i++
						val += int(block[i]-'0') + val*10
						val *= sign
						batch = append(batch, HKV{HK: HK{Hash: uint(xxh3.Hash(key)), Key: key}, Value: val})
						if len(batch) >= HKV_BATCH {
							t.SendBatches = time.Now()
							chanBatch <- batch
							batch = make(Batch, 0, HKV_BATCH)
							t.Since_SendBatches.Since(t.SendBatches)
						}

						for ; block[i] != '\n'; i++ {
						}
					}
				}
				t.SendBatches = time.Now()
				chanBatch <- batch
				t.Since_SendBatches.Since(t.SendBatches)
				close(chanBatch)
				wg.Done()
			}(t)
		}

		tWaitParse := time.Now()
		wg.Wait()
		t.Since_WaitParse = time.Since(tWaitParse)

		close(chanChanBatch)
		t.Since_ParseBlock = time.Since(t.ParseBlocks)
	}(t)
	return chanChanBatch
}

func MapData(chanChanBatch chan chan Batch, t *Timings) (chanOutput chan OutputMap) {
	t.MapData = time.Now()
	chanOutput = make(chan OutputMap, CHANS*16)
	var wg sync.WaitGroup
	wg.Add(CHANS)
	go func(t *Timings) {
		for chanBatch := range chanChanBatch {
			go func(t *Timings) {
				output := make(OutputMap, MAP_SIZE)
				for batch := range chanBatch {
					for _, hkv := range batch {
						val := hkv.Value
						data, ok := output[hkv.Hash]
						if !ok {
							output[hkv.Hash] = &CityData{

								Min:   val,
								Sum:   val,
								Max:   val,
								Count: 1,
								HK:    hkv.HK,
							}
							continue
						}

						data.Min = min(data.Min, val)
						data.Max = max(data.Max, val)
						data.Sum += val
						data.Count++
					}
				}
				tSendOutput := time.Now()
				chanOutput <- output
				t.SendOutput.Since(tSendOutput)
				wg.Done()
			}(t)
		}

		tWaitMap := time.Now()
		wg.Wait()
		t.Since_WaitMap = time.Since(tWaitMap)

		close(chanOutput)
		t.Since_MapData = time.Since(t.MapData)
	}(t)

	return chanOutput
}

func MergeMaps(chanOutput chan OutputMap, t *Timings) OutputMap {
	output := make(OutputMap, MAP_SIZE)
	tMergeWait := time.Now()
	for subOutput := range chanOutput {
		t.Merge = time.Now()
		if len(output) == 0 {
			output = subOutput
			t.Since_Merge += time.Since(t.Merge)
			continue
		}
		for k, v := range subOutput {
			if v0, ok := output[k]; ok {
				v0.Merge(v)
			} else {
				output[k] = v
			}
		}
		t.Since_Merge += time.Since(t.Merge)
	}
	t.Since_MergeWait = time.Since(tMergeWait)
	return output
}

func PrintOutput(output OutputMap, t *Timings) {
	tSort := time.Now()
	hks := make([]HK, 0, len(output))
	for _, v := range output {
		hks = append(hks, v.HK)
	}

	sort.Slice(hks, func(i, j int) bool {
		ki, kj := hks[i].Key, hks[j].Key
		for k := 0; k < len(ki) && k < len(kj); k++ {
			if ki[k] != kj[k] {
				return ki[k] < kj[k]
			}
		}
		return len(ki) < len(kj)
	})
	t.Since_Sort = time.Since(tSort)

	tBuild := time.Now()
	var sb strings.Builder
	for _, k := range hks {
		data := output[k.Hash]
		fmt.Fprintf(&sb, "%s=%s/%s/%s\n", k.Key,
			pkg.PrintIndec(data.Min), pkg.PrintIndec(data.Sum/data.Count), pkg.PrintIndec(data.Max))
	}
	t.Since_Build = time.Since(tBuild)

	tPrint := time.Now()
	os.Stdout.WriteString(sb.String())
	t.Since_Print = time.Since(tPrint)
}
