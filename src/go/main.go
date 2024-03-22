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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	READ_BUF = 1024000
	CHANS    = 64
	CHAN_BUF = 100
	MAP_SIZE = 1000
)

type CityData struct {
	Min, Sum, Max int
	Count         int
	Lock          *sync.Mutex
}

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

	file, err := os.Open("../../data/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var (
		wg         *sync.WaitGroup = &sync.WaitGroup{}
		mapLock    *sync.RWMutex   = &sync.RWMutex{}
		blockChans []chan []byte   = make([]chan []byte, CHANS)

		output = make(map[string]*CityData, MAP_SIZE)
	)
	wg.Add(CHANS)
	for i := range CHANS {
		blockChans[i] = make(chan []byte, CHAN_BUF)
		go func() {
			defer wg.Done()
			var (
				line  string
				data  *CityData
				ok    bool
				key   string
				val   int
				block []byte
			)
			for block = range blockChans[i] {
				newlineIndex := bytes.IndexByte(block, '\n')
				for {
					if newlineIndex <= 0 {
						break
					}

					line = string(block[:newlineIndex])
					key, val = SplitParse(line)
					mapLock.RLock()
					data, ok = output[key]
					mapLock.RUnlock()
					if !ok {
						data = &CityData{val, val, val, 1, &sync.Mutex{}}
						mapLock.Lock()
						output[key] = data
						mapLock.Unlock()
						continue
					}

					data.Lock.Lock()
					if val < data.Min {
						data.Min = val
					}
					if val > data.Max {
						data.Max = val
					}
					data.Sum += val
					data.Count++
					data.Lock.Unlock()
					block = block[newlineIndex+1:]
					newlineIndex = bytes.IndexByte(block, '\n')
				}
			}
		}()
	}

	var (
		tRead = time.Now()
		buf   = make([]byte, READ_BUF)

		off, i       int64
		n, chanIndex int
	)
	for i = int64(0); ; i += int64(n) {
		if off>>4 >= *flagN {
			break
		}

		n, err = file.ReadAt(buf, off)
		for n = n - 1; n >= 0; n-- {
			if buf[n] == '\n' {
				break
			}
		}
		off += int64(n) + 1
		blockChans[chanIndex] <- bytes.Clone(buf[:n])
		if err == io.EOF {
			break
		}
		chanIndex = (chanIndex + 1) % CHANS
	}

	for i := range CHANS {
		close(blockChans[i])
	}

	since_tRead := time.Since(tRead)
	tWait := time.Now()
	wg.Wait()
	since_tWait := time.Since(tWait)

	var data *CityData
	var tSort, tPrint time.Time
	var since_tSort, since_tPrint time.Duration
	tSort = time.Now()
	keys := make([]string, 0, len(output))
	for k := range output {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	since_tSort += time.Since(tSort)

	tPrint = time.Now()
	var sb strings.Builder
	for _, k := range keys {
		data = output[k]
		fmt.Fprintf(&sb, "%s=%.1f/%.1f/%.1f\n", k,
			float64(data.Min)/10, float64(data.Sum)/float64(data.Count)/10, float64(data.Max)/10)
	}

	os.Stdout.WriteString(sb.String())
	since_tPrint += time.Since(tPrint)

	since_tTotal := time.Since(tTotal)
	log.Printf(`
= Reading Took: %v
  - Waiting: %v
= Sorting Took: %v
= Printing Took: %v
= Total Took: %v
 	`,
		since_tRead,
		since_tWait,
		since_tSort,
		since_tPrint,
		since_tTotal,
	)
}

func SplitParse(line string) (key string, val int) {
	i := strings.IndexByte(line, ';')
	save := strings.Clone(line)
	key, line = line[:i], line[i+1:]
	_ = save
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
