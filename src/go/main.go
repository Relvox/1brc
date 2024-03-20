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
	Min, Sum, Max float64
	Count         int
}

func main() {
	t0 := time.Now()
	defer func() { log.Println("= Total Took:", time.Since(t0)) }()

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
		cities  = make(map[string]CityData)
		t1      = time.Now()

		line  string
		data  CityData
		ok    bool
		parts []string
		val   float64
	)

	for i := int64(0); scanner.Scan() && i < *flagN; i++ {
		line = scanner.Text()

		parts = strings.Split(line, ";")
		val, err = strconv.ParseFloat(parts[1], 64)
		if len(parts) != 2 || err != nil {
			log.Println("eh?", parts, err)
			continue
		}

		data, ok = cities[parts[0]]
		if !ok {
			data = CityData{val, val, val, 1}
			cities[parts[0]] = data
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
		cities[parts[0]] = data
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	since_t1 := time.Since(t1)
	var t2, t3 time.Time
	var since_t2, since_t3 time.Duration
	t2 = time.Now()

	keys := make([]string, 0, len(cities))
	for k := range cities {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	since_t2 = time.Since(t2)
	t3 = time.Now()
	for _, k := range keys {
		data = cities[k]
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", k, data.Min, data.Sum/float64(data.Count), data.Max)
	}

	since_t3 = time.Since(t3)

	log.Println("= Scanning Took:", since_t1)
	log.Println("= Sorting Took:", since_t2)
	log.Println("= Printing Took:", since_t3)
}
