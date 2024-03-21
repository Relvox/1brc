package main

import (
	"bufio"
	"flag"
	"fmt"
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

type CityData struct {
	Min, Sum, Max int
	Count         int
}

const (
	CHANS     = 255
	CHAN_SIZE = 100_000
)

func main() {
	tTotal := time.Now()

	flagProf := flag.String("cprof", "", "write cpu profile to file")
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

	var (
		cityMaps  = make([]map[string]*CityData, CHANS)
		cityChans = make([]chan string, CHANS)

		wg *sync.WaitGroup = &sync.WaitGroup{}
	)

	wg.Add(CHANS)
	for i := range CHANS {
		cityMaps[i] = make(map[string]*CityData, CHAN_SIZE)
		cityChans[i] = make(chan string, CHAN_SIZE)
		go Map(cityChans[i], cityMaps[i], wg)
	}

	file, err := os.Open("../../data/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	var (
		scanner = bufio.NewScanner(file)

		line string
		data *CityData

		tRead, tProcess             time.Time
		since_tRead, since_tProcess time.Duration
	)

	tScan := time.Now()
	for i := int64(0); scanner.Scan() && i < *flagN; i++ {
		tRead = time.Now()
		line = scanner.Text()
		since_tRead += time.Since(tRead)
		tProcess = time.Now()
		cityChans[line[0]] <- line
		since_tProcess += time.Since(tProcess)
	}

	for i := range CHANS {
		close(cityChans[i])
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	tWait := time.Now()
	wg.Wait()
	since_tScan := time.Since(tScan)
	since_tWait := time.Since(tWait)
	var tSort, tPrint time.Time
	var since_tSort, since_tPrint time.Duration
	for i := range CHANS {
		tSort = time.Now()
		keys := make([]string, 0, len(cityMaps[i]))
		for k := range cityMaps[i] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		since_tSort += time.Since(tSort)

		tPrint = time.Now()
		var sb strings.Builder
		for _, k := range keys {
			data = cityMaps[i][k]
			fmt.Fprintf(&sb, "%s=%.1f/%.1f/%.1f\n", k,
				float64(data.Min)/10, float64(data.Sum)/float64(data.Count)/10, float64(data.Max)/10)
		}

		os.Stdout.WriteString(sb.String())
		since_tPrint += time.Since(tPrint)
	}
	since_tTotal := time.Since(tTotal)
	log.Printf(`
= Scanning Took: %v
  - Reading: %v
  - Process: %v
  - Waiting: %v
= Sorting Took: %v
= Printing Took: %v
= Total Took: %v
 	`,
		since_tScan,
		since_tRead,
		since_tProcess,
		since_tWait,
		since_tSort,
		since_tPrint,
		since_tTotal,
	)
}

func Map(input chan string, output map[string]*CityData, wg *sync.WaitGroup) {
	var line string
	var data *CityData
	var ok bool
	defer wg.Done()
	for line = range input {
		key, val := SplitParse(line)
		data, ok = output[key]
		if !ok {
			data = &CityData{val, val, val, 1}
			output[key] = data
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
