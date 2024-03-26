package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
)

const (
	READ_BUF = 1024 * 1024
)

type CityData struct {
	Min, Sum, Max float64
	Count         int
}

var debugData map[string][]float64 = make(map[string][]float64)

func main() {
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

	file, err := os.Open("../../../data/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	truMap := map[string][]float64{}

	d := 0
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, READ_BUF), READ_BUF)
	for i := int64(0); scanner.Scan() && i < *flagN; i++ {
		if i%10_000_000 == 0 {
			d++
			fmt.Print(d % 10)
		}
		line := string(scanner.Bytes())
		kv := strings.Split(line, ";")
		key := kv[0]
		val, _ := strconv.ParseFloat(kv[1], 64)

		truMap[key] = append(truMap[key], val)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("")
	// var tSort, tPrint time.Time
	// var since_tSort, since_tPrint time.Duration
	// tSort = time.Now()

	// keys := make([]string, 0, len(cities))
	// for k := range cities {
	// 	keys = append(keys, k)
	// }
	// sort.Strings(keys)

	// since_tSort = time.Since(tSort)
	// tPrint = time.Now()
	var sb strings.Builder
	for k, v := range truMap {
		min, max, sum := 100., -100., 0.
		for _, v := range v {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
			sum += v
		}
		fmt.Fprintf(&sb, "%s:\n  min: %.1f max: %.1f sum: %.1f count: %d avg %.1f:\n", k, min, max, sum, len(v), sum/float64(len(v)))
	}
	os.Stdout.WriteString(sb.String())

}
