package main

import (
	"bytes"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
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
type HKV struct {
	Hash  HashKey
	Key   []byte
	Value int
}

type Batch = []HKV

type OutputMap = map[HashKey]*CityData

type CityData struct {
	Min, Sum, Max int
	Count         int
	Name          []byte
}

func (cd *CityData) Merge(other *CityData) {
	if other == nil {
		return
	}

	cd.Min = min(cd.Min, other.Min)
	cd.Max = max(cd.Max, other.Max)
	cd.Sum += other.Sum
	cd.Count += other.Count
}

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
	flagN := flag.Int64("n", 1_000_000_000, "max n")

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

	chanChanBlock := ReadFile(*flagN)
	chanChanBatch := ParseBlocks(chanChanBlock)
	chanOutput := MapData(chanChanBatch)

	tMerge := time.Now()

	output := MergeOutput(chanOutput)
	since_tMerge = time.Since(tMerge)
	since_tSort, since_tPrintPrep, since_tPrint = PrintOutput(output)

	log.Printf(`
[ Read: %v
[ Parse: %v
  > Wait: %v
[ MapData: %v
  > Wait: %v
? Merge Maps: %v
? Sorting Took: %v
? Print Prep: %v
? Printing: %v
= Total Took: %v
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

func MergeOutput(chanOutput chan OutputMap) OutputMap {
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
	return output
}

func ReadFile(flagN int64) (chanChanBlock chan chan []byte) {
	tReadFile := time.Now()
	chanChanBlock = make(chan chan []byte, CHANS)
	chanBlocks := [CHANS]chan []byte{}

	go func() {
		file, err := os.Open("../../data/measurements.txt")
		if err != nil {
			panic(err)
		}

		fi, _ := file.Stat()
		size := fi.Size()
		low, high := uint32(size), uint32(size>>32)
		fmap, err := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, syscall.PAGE_READONLY, high, low, nil)
		if err != nil {
			panic(err)
		}
		defer syscall.CloseHandle(fmap)
		ptr, err := syscall.MapViewOfFile(fmap, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
		if err != nil {
			panic(err)
		}

		data := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), size)

		var chanIndex int
		for off := int64(0); off>>4 < flagN; {
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
			close(chanBlocks[i])
		}
		close(chanChanBlock)
		since_tReadFile += time.Since(tReadFile)
	}()

	return chanChanBlock
}

func SplitParse(line []byte) (key []byte, val int) {
	semiColonIndex := bytes.IndexByte(line, ';')
	dotIndex := bytes.IndexByte(line, '.')
	key = line[:semiColonIndex]
	neg := line[semiColonIndex+1] == '-'
	startValIndex := semiColonIndex + 1
	if neg {
		startValIndex++
	}

	for i := startValIndex; i < dotIndex; i++ {
		val = val*10 + int(line[i]-'0')
	}

	if dotIndex+1 < len(line) {
		val = val*10 + int(line[dotIndex+1]-'0')
	}

	if neg {
		val = -val
	}

	return
}

func ParseBlocks(chanChanBlock chan chan []byte) (chanChanBatch chan chan []HKV) {
	tParseBlock := time.Now()
	chanChanBatch = make(chan chan []HKV, CHANS)

	var wg sync.WaitGroup
	wg.Add(CHANS)
	go func() {
		for chanBlock := range chanChanBlock {
			go func() {
				chanBatch := make(chan Batch, BATCH_CHAN_BUF)
				chanChanBatch <- chanBatch
				batch := make([]HKV, 0, HKV_BATCH)
				var key []byte
				var val int
				for block := range chanBlock {
					for {
						m := bytes.IndexByte(block, 10)
						if m < 0 {
							key, val = SplitParse(block)
						} else {
							key, val = SplitParse(block[:m])
						}

						batch = append(batch, HKV{Hash(key), key, val})
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
							data = &CityData{kvp.Value, kvp.Value, kvp.Value, 1, kvp.Key}
							output[kvp.Hash] = data
							continue
						}

						if kvp.Value < data.Min {
							data.Min = kvp.Value
						}
						if kvp.Value > data.Max {
							data.Max = kvp.Value
						}
						data.Sum += kvp.Value
						data.Count++
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
		fmt.Fprintf(&sb, "%s=%.1f/%.1f/%.1f\n", k.Key,
			float64(data.Min)/10, float64(data.Sum)/float64(data.Count)/10, float64(data.Max)/10)
	}
	since_tPrintPrep := time.Since(tPrintPrep)

	tPrint := time.Now()
	os.Stdout.WriteString(sb.String())
	since_tPrint := time.Since(tPrint)

	return since_tSort, since_tPrintPrep, since_tPrint
}

func Hash(s []byte) HashKey {
	h := fnv.New32a()
	h.Write(s)
	res := h.Sum32()
	return res
}
