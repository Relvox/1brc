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
	READ_BUF      = 1024000
	READ_CHANS    = 5
	READ_CHAN_BUF = 20
	MAP_CHANS     = 256
	MAP_CHAN_BUF  = 100
	MAP_SIZE      = 2000
)

type CityData struct {
	Name          string
	Min, Sum, Max int
	Count         int
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
		wgRead *sync.WaitGroup = &sync.WaitGroup{}
		wgMaps *sync.WaitGroup = &sync.WaitGroup{}

		blockChans []chan []byte = make([]chan []byte, READ_CHANS)

		mapChans []chan *CityData       = make([]chan *CityData, MAP_CHANS)
		resMaps  []map[string]*CityData = make([]map[string]*CityData, MAP_CHANS)
	)

	wgMaps.Add(MAP_CHANS)
	for i := range MAP_CHANS {
		mapChans[i] = make(chan *CityData, MAP_CHAN_BUF)
		resMaps[i] = make(map[string]*CityData, MAP_SIZE)
		go func() {
			defer wgMaps.Done()
			var (
				data *CityData
				ok   bool
				val  int
				key  string
			)
			for data = range mapChans[i] {
				key = data.Name
				_, ok = resMaps[i][key]
				if !ok {
					resMaps[i][key] = data
					continue
				}
				if val < data.Min {
					data.Min = val
				}
				if val > data.Max {
					data.Max = val
				}
				data.Sum += val
				data.Count++
			}
		}()
	}

	wgRead.Add(READ_CHANS)
	for i := range READ_CHANS {
		blockChans[i] = make(chan []byte, READ_CHAN_BUF)
		go func() {
			defer wgRead.Done()
			var (
				// output map[string]*CityData = chanMaps[i]
				line    string
				data    *CityData
				key     string
				chanKey byte
				val     int
				block   []byte
			)
			for block = range blockChans[i] {
				newlineIndex := bytes.IndexByte(block, '\n')
				for {
					if newlineIndex <= 0 {
						break
					}

					line = string(block[:newlineIndex])
					key, val = SplitParse(line)
					data = &CityData{key, val, val, val, 1}
					chanKey = key[0]
					mapChans[chanKey] <- data

					block = block[newlineIndex+1:]
					newlineIndex = bytes.IndexByte(block, '\n')
				}
			}
		}()
	}

	var (
		tRead = time.Now()
		buf   = make([]byte, READ_BUF)

		off,i          int64
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
		chanIndex = (chanIndex + 1) % READ_CHANS
	}

	for i := range READ_CHANS {
		close(blockChans[i])
	}

	since_tRead := time.Since(tRead)
	tWait := time.Now()
	wgRead.Wait()
	for i := range MAP_CHANS {
		close(mapChans[i])
	}
	wgMaps.Wait()
	since_tWait := time.Since(tWait)

	var data *CityData
	var tSort, tPrint time.Time
	var since_tSort, since_tPrint time.Duration
	tSort = time.Now()
	var k string
	for i := range MAP_CHANS {
		keys := make([]string, 0, len(resMaps[i]))
		for k = range resMaps[i] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		since_tSort += time.Since(tSort)

		tPrint = time.Now()
		var sb strings.Builder
		for _, k = range keys {
			data = resMaps[i][k]
			fmt.Fprintf(&sb, "%s=%.1f/%.1f/%.1f\n", k,
				float64(data.Min)/10, float64(data.Sum)/float64(data.Count)/10, float64(data.Max)/10)
		}

		os.Stdout.WriteString(sb.String())
		since_tPrint += time.Since(tPrint)
		tSort = time.Now()
	}

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
	var rest, units string
	i := strings.IndexByte(line, ';')
	key, line = line[:i], line[i+1:]
	i = strings.IndexByte(line, '.')
	rest, units = line[:i], line[i+1:]
	if units == "" {
		units = "0"
	}
	
	var val64 int64
	var err error
	val64, err = strconv.ParseInt(rest+units, 10, 32)
	if err != nil {
		panic(fmt.Errorf("parsing '%s': %w", rest+units, err))
	}

	val = int(val64)
	return
}
