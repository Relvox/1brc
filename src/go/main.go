package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"
)

type CityData struct {
	Min, Sum, Max int
	Count         int
}

func main() {
	tTotal := time.Now()

	flagProf := flag.String("cprof", "", "write cpu profile to file")
	flagN := flag.Int64("n", 1_000_000_000, "max n")

	flag.Parse()

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
		scanner = bufio.NewScanner(file)
		cities  = make(map[string]*CityData)

		line string
		data *CityData
		ok   bool
		val  int
		key  string

		tScan, tRead, tProcess      time.Time
		since_tRead, since_tProcess time.Duration
	)

	tScan = time.Now()
	tRead = tScan
	for i := int64(0); scanner.Scan() && i < *flagN; i++ {
		line = scanner.Text()
		since_tRead += time.Since(tRead)
		tProcess = time.Now()
		key, val = SplitParse(line)
		data, ok = cities[key]
		if !ok {
			data = &CityData{val, val, val, 1}
			cities[key] = data
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

		since_tProcess += time.Since(tProcess)
		tRead = time.Now()
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	since_tScan := time.Since(tScan)
	var tSort, tPrint time.Time
	var since_tSort, since_tPrint time.Duration
	tSort = time.Now()

	keys := make([]string, 0, len(cities))
	for k := range cities {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	since_tSort = time.Since(tSort)
	tPrint = time.Now()
	var sb strings.Builder
	for _, k := range keys {
		data = cities[k]
		fmt.Fprintf(&sb, "%s=%.1f/%.1f/%.1f\n", k,
			float64(data.Min)/10, float64(data.Sum)/float64(data.Count)/10, float64(data.Max)/10)
	}

	os.Stdout.WriteString(sb.String())
	since_tPrint = time.Since(tPrint)
	since_tTotal := time.Since(tTotal)
	log.Printf(`
= Scanning Took: %v
  - Reading: %v
  - Processing: %v
= Sorting Took: %v
= Printing Took: %v
= Total Took: %v
 	`,
		since_tScan,
		since_tRead,
		since_tProcess,
		since_tSort,
		since_tPrint,
		since_tTotal,
	)
}

func SplitParse(line string) (key string, val int) {
	i := strings.IndexByte(line, ';')
	key = line[:i]
	ll := len(line)
	rest := line[i+1:ll-2] + line[ll-1:]

	val64, err := strconv.ParseInt(rest, 10, 32)
	if err != nil {
		panic(err)
	}

	val = int(val64)
	return
}
