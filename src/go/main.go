package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	READ_BUF = 1024 * 1024
	CHANS    = 10

	BLOCK_CHAN_BUF = 64
	KVP_BATCH      = 512
	KVP_CHAN_BUF   = 64

	MAP_SIZE = 512
)

type KVP struct {
	Key   string
	Value int
}

type CityData struct {
	Min, Sum, Max int
	Count         int
	Lock          *sync.Mutex
}

func (cd *CityData) Merge(other *CityData) *CityData {
	if cd == nil {
		return other
	}

	cd.Min = min(cd.Min, other.Min)
	cd.Max = max(cd.Max, other.Max)
	cd.Sum = cd.Sum + other.Sum
	cd.Count = cd.Count + other.Count
	return cd
}

var (
	since_tRead       time.Duration
	since_tParseBlock time.Duration
	since_tMerge      time.Duration
	since_tSort       time.Duration
	since_tPrintPrep  time.Duration
	since_tPrint      time.Duration
)

func main() {
	tTotal := time.Now()
	tSetup := time.Now()
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
	since_tSetup := time.Since(tSetup)

	chanChanBlock := ReadFile(*flagN)

	tParseBlock := time.Now()
	var wgInner *sync.WaitGroup = &sync.WaitGroup{}
	var outputs []map[string]*CityData
	for chanBlock := range chanChanBlock {
		wgInner.Add(1)
		go func() {
			chanBatch := ParseBlock(chanBlock)
			output := MapData(chanBatch)
			outputs = append(outputs, output)
			wgInner.Done()
		}()
	}

	tWaitInner := time.Now()
	wgInner.Wait()
	since_tWaitInner := time.Since(tWaitInner)
	since_tParseBlock = time.Since(tParseBlock)

	tMerge := time.Now()
	output := outputs[0]

	for _, o := range outputs[1:] {
		for k, v := range o {
			output[k] = output[k].Merge(v)
		}
	}
	since_tMerge = time.Since(tMerge)

	since_tSort, since_tPrintPrep, since_tPrint = PrintOutput(output)

	log.Printf(`
[ Setup: %v
~ Read: %v
| Wait Inner: %v
~ Parse Block: %v
? Merge Maps: %v
? Sorting Took: %v
? Print Prep: %v
? Printing: %v
= Total Took: %v
		 `,
		since_tSetup,
		since_tRead,
		since_tWaitInner,
		since_tParseBlock,
		since_tMerge,
		since_tSort,
		since_tPrintPrep,
		since_tPrint,
		time.Since(tTotal),
	)
}

func ReadFile(flagN int64) (chanChanBlock chan chan []byte) {
	tRead := time.Now()
	chanChanBlock = make(chan chan []byte, CHANS)
	chanBlocks := [CHANS]chan []byte{}

	go func() {
		file, err := os.Open("../../data/measurements.txt")
		if err != nil {
			panic(err)
		}

		var (
			off       int64
			chanIndex int
			n         int
		)

		for i := int64(0); ; i += int64(n) {
			if off>>4 >= flagN {
				break
			}

			buf := make([]byte, READ_BUF)
			n, err = file.ReadAt(buf, off)
			for n = n - 1; n >= 0; n-- {
				if buf[n] == '\n' {
					break
				}
			}

			off += int64(n) + 1
			if chanBlocks[chanIndex] == nil {
				chanBlocks[chanIndex] = make(chan []byte, BLOCK_CHAN_BUF)
				chanChanBlock <- chanBlocks[chanIndex]
			}

			chanBlocks[chanIndex] <- buf[:n]
			if err == io.EOF {
				break
			}

			chanIndex = (chanIndex + 1) % CHANS
		}

		for i := range CHANS {
			close(chanBlocks[i])
		}
		close(chanChanBlock)
		since_tRead = time.Since(tRead)
	}()
	return chanChanBlock
}

func SplitParse(line string) (key string, val int) {
	i := strings.IndexByte(line, ';')
	key, line = line[:i], line[i+1:]
	i = strings.IndexByte(line, '.')
	rest, units := line[:i], line[i+1:]
	if units == "" {
		units = "0"
	}

	val64, err := strconv.ParseInt(rest+units, 10, 32)
	if err != nil {
		panic(fmt.Errorf("parsing '%s': %w", rest+units, err))
	}

	val = int(val64)
	return
}

func ParseBlock(chanBlock chan []byte) (chanBatch chan []KVP) {
	batch := make([]KVP, 0, KVP_BATCH)
	sep := []byte{10}
	chanBatch = make(chan []KVP, KVP_CHAN_BUF)
	go func() {
		for block := range chanBlock {
			for {
				m := bytes.Index(block, sep)
				if m < 0 {
					break
				}

				key, val := SplitParse(string(block[:m:m]))
				batch = append(batch, KVP{key, val})
				if len(batch) >= KVP_BATCH {
					chanBatch <- batch
					batch = make([]KVP, 0, KVP_BATCH)
				}
				block = block[m+1:]
			}

			key, val := SplitParse(string(block))
			batch = append(batch, KVP{key, val})
			if len(batch) >= KVP_BATCH {
				chanBatch <- batch
				batch = make([]KVP, 0, KVP_BATCH)
			}
		}

		chanBatch <- slices.Clip(batch)
		close(chanBatch)
	}()

	return chanBatch
}

func MapData(chanBatch chan []KVP) (output map[string]*CityData) {
	output = make(map[string]*CityData, 0)
	for kvps := range chanBatch {
		for _, kvp := range kvps {
			data, ok := output[kvp.Key]

			if !ok {
				data = &CityData{kvp.Value, kvp.Value, kvp.Value, 1, &sync.Mutex{}}
				output[kvp.Key] = data
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
	return output
}

func PrintOutput(output map[string]*CityData) (time.Duration, time.Duration, time.Duration) {
	tSort := time.Now()
	keys := make([]string, 0, len(output))
	for k := range output {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	since_tSort := time.Since(tSort)

	tPrintPrep := time.Now()
	var sb strings.Builder
	for _, k := range keys {
		data := output[k]
		fmt.Fprintf(&sb, "%s=%.1f/%.1f/%.1f\n", k,
			float64(data.Min)/10, float64(data.Sum)/float64(data.Count)/10, float64(data.Max)/10)
	}
	since_tPrintPrep := time.Since(tPrintPrep)

	tPrint := time.Now()
	os.Stdout.WriteString(sb.String())
	since_tPrint := time.Since(tPrint)

	return since_tSort, since_tPrintPrep, since_tPrint
}
