package main

import (
	"brc/pkg"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	READ_BUF = 1024 * 1024 * 4
	CHANS    = 12

	BLOCK_CHAN_BUF = 64
	BATCH_CHAN_BUF = 64
	MAP_CHAN_BUF   = 64

	HKV_BATCH = 512 * 4

	MAP_SIZE = 41_343
)

type HashKey = uint32

type HK struct {
	Hash HashKey
	Key  []byte
}

type Batch = []pkg.HKV

type OutputMap = map[HashKey]*pkg.CityData

var (
	since_tReadFile   time.Duration
	since_tParseBlock time.Duration
	since_tWaitParse  time.Duration
	since_tMapData    time.Duration
	since_tWaitMap    time.Duration
	since_tMerge      time.Duration
	since_tSort       time.Duration
	since_tPrintPrep  time.Duration
	since_tPrint      time.Duration
)

func main() {
	tTotal := time.Now()
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

	log.Println(*flagProf, *flagTrace, *flagFile, *flagPercent)
	chanChanBlock := ReadFile(*flagFile, *flagPercent)
	chanChanBatch := ParseBlocks(chanChanBlock)
	chanOutput := MapData(chanChanBatch)

	tMerge := time.Now()
	output := make(OutputMap, MAP_SIZE)
	for subOutput := range chanOutput {
		if len(output) == 0 {
			output = subOutput
			continue
		}
		for k, v := range subOutput {
			if v0, ok := output[k]; ok {
				v0.Merge(v)
			} else {
				output[k] = v
			}
		}
	}
	since_tMerge = time.Since(tMerge)

	since_tSort, since_tPrintPrep, since_tPrint = PrintOutput(output)

	log.Printf(`
[ Read: %v
[ Parse: %v
  > Wait: %v
[ MapData: %v
  > Wait: %v
? Merge Maps: %v
? Sorting: %v
? Print Prep: %v
? Printing: %v
= Total: %v
		 `,
		since_tReadFile,
		since_tParseBlock,
		since_tWaitParse,
		since_tMapData,
		since_tWaitMap,
		since_tMerge,
		since_tSort,
		since_tPrintPrep,
		since_tPrint,
		time.Since(tTotal),
	)
}

func ReadFile(file string, percent int) (chanChanBlock chan chan []byte) {
	tReadFile := time.Now()
	chanChanBlock = make(chan chan []byte, CHANS)
	chanBlocks := [CHANS]chan []byte{}

	go func() {
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

			off += int64(n) + 1
			since_tReadFile += time.Since(tReadFile)
			if chanBlocks[chanIndex] == nil {
				chanBlocks[chanIndex] = make(chan []byte, BLOCK_CHAN_BUF)
				chanChanBlock <- chanBlocks[chanIndex]
			}

			chanBlocks[chanIndex] <- buf[:n]
			tReadFile = time.Now()

			chanIndex = (chanIndex + 1) % CHANS
		}

		for i := range CHANS {
			if chanBlocks[i] == nil {
				chanBlocks[i] = make(chan []byte)
				chanChanBlock <- chanBlocks[chanIndex]
			}
			close(chanBlocks[i])
		}
		close(chanChanBlock)
		since_tReadFile += time.Since(tReadFile)
	}()

	return chanChanBlock
}

func ParseBlocks(chanChanBlock chan chan []byte) (chanChanBatch chan chan []pkg.HKV) {
	tParseBlock := time.Now()
	chanChanBatch = make(chan chan []pkg.HKV, CHANS)

	var wg sync.WaitGroup
	wg.Add(CHANS)
	go func() {
		for chanBlock := range chanChanBlock {
			go func() {
				chanBatch := make(chan Batch, BATCH_CHAN_BUF)
				chanChanBatch <- chanBatch
				batch := make([]pkg.HKV, 0, HKV_BATCH)
				var key []byte
				var val int
				for block := range chanBlock {
					for {
						m := bytes.IndexByte(block, 10)
						if m < 0 {
							key, val = pkg.SplitParse(block)
						} else {
							key, val = pkg.SplitParse(block[:m])
						}

						batch = append(batch, pkg.HKV{pkg.Hash(key), key, val})
						if len(batch) >= HKV_BATCH {
							chanBatch <- batch
							batch = make(Batch, 0, HKV_BATCH)
						}
						if m < 0 {
							break
						}
						block = block[m+1:]
					}
				}

				chanBatch <- batch
				close(chanBatch)
				wg.Done()
			}()
		}

		tWaitParse := time.Now()
		wg.Wait()
		since_tWaitParse = time.Since(tWaitParse)

		close(chanChanBatch)
		since_tParseBlock = time.Since(tParseBlock)
	}()

	return chanChanBatch
}

func MapData(chanChanBatch chan chan Batch) (chanOutput chan OutputMap) {
	tMapData := time.Now()
	chanOutput = make(chan OutputMap, MAP_CHAN_BUF)
	var wg sync.WaitGroup
	wg.Add(CHANS)
	go func() {
		for chanBatch := range chanChanBatch {
			go func() {
				output := make(OutputMap, HKV_BATCH)
				for kvps := range chanBatch {
					for _, kvp := range kvps {
						data, ok := output[kvp.Hash]
						if !ok {
							output[kvp.Hash] = &pkg.CityData{
								kvp.Value,
								kvp.Value,
								kvp.Value,
								1,
								kvp.Key,
							}
							continue
						}

						data.MergeValue(kvp.Value)
					}
				}

				chanOutput <- output
				wg.Done()
			}()
		}

		tWaitMap := time.Now()
		wg.Wait()
		since_tWaitMap = time.Since(tWaitMap)

		close(chanOutput)
		since_tMapData = time.Since(tMapData)
	}()

	return chanOutput
}

func PrintOutput(output OutputMap) (time.Duration, time.Duration, time.Duration) {
	tSort := time.Now()
	names := make([]HK, 0, len(output))
	for h, v := range output {
		names = append(names, HK{h, v.Name})
	}

	sort.Slice(names, func(i, j int) bool {
		ki, kj := names[i].Key, names[j].Key
		for k := 0; k < len(ki) && k < len(kj); k++ {
			if ki[k] != kj[k] {
				return ki[k] < kj[k]
			}
		}
		return len(ki) < len(kj)
	})
	since_tSort := time.Since(tSort)

	tPrintPrep := time.Now()
	var sb strings.Builder
	for _, k := range names {
		data := output[k.Hash]
		fmt.Fprintf(&sb, "%s=%s/%s/%s\n", k.Key,
			pkg.PrintIndec(data.Min), pkg.PrintIndec(data.Sum/data.Count), pkg.PrintIndec(data.Max))
	}
	since_tPrintPrep := time.Since(tPrintPrep)

	tPrint := time.Now()
	os.Stdout.WriteString(sb.String())
	since_tPrint := time.Since(tPrint)

	return since_tSort, since_tPrintPrep, since_tPrint
}
