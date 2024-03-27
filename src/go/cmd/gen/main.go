package main

import (
	"brc/pkg"
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/maps"
)

type Station struct {
	Target   int
	ValCount int
	ValSum   int
	Vals     []int
}

const SPREAD = 123

func (s Station) MinMax() (int, int) {
	return s.Target - SPREAD, s.Target + SPREAD
}

func (s Station) NaiveValue(rng *rand.Rand) int {
	return s.Target + (rng.Intn(2*SPREAD+1) - SPREAD)
}

func (s Station) BiasedValue(rng *rand.Rand, sign, error int) int {
	rv := 61 + rng.Intn(61)
	return s.Target + sign*min(rv, sign*error*s.ValCount)
}

type StationMap = map[string]*Station

func AddValue(cityStation *Station, v int) {
	cityStation.ValSum += v
	cityStation.Vals = append(cityStation.Vals, v)
	cityStation.ValCount++
}

var (
	since_tReadFile     time.Duration
	since_tBaseStations time.Duration
	since_tSort         time.Duration

	since_tBulk     time.Duration
	since_tBulkPrep time.Duration
	since_tBulkInit time.Duration
	since_tBulkBulk time.Duration

	since_tErrAvg time.Duration
	since_tErrSum time.Duration
	since_tFill   time.Duration

	since_tWriteCheck time.Duration
	since_tDerange    time.Duration
	since_tOutput     time.Duration
)

func main() {
	tTotal := time.Now()
	flagProf := flag.String("prof", "", "write cpu profile to file")
	flagTrace := flag.String("trace", "", "write trace to file")

	flagInput := flag.String("input", "../../data/weather_stations.csv", "1brc input file")
	flagFile := flag.String("file", "../../data/measurements.txt", "1brc file")
	flagCheck := flag.String("check", "../../data/measurements.chk", "1brc check file")

	flagN := flag.Int64("n", 1_000_000_000, "rows")
	flagBulk := flag.Int("bulk", 90, "% bulk")
	flagSeed := flag.Int64("seed", 0, "rng seed")
	flag.Parse()

	if *flagTrace != "" {
		f, _ := os.OpenFile(*flagTrace, os.O_CREATE|os.O_TRUNC, 0644)
		log.Printf("starting trace: '%s'", *flagTrace)
		trace.Start(f)
		defer f.Close()
		defer trace.Stop()
	}
	if *flagProf != "" {
		f, _ := os.Create(*flagProf)
		log.Printf("starting cpu-prof: '%s'", *flagProf)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	tReadFile := time.Now()
	log.Printf("reading input file: '%s'", *flagInput)
	inputBytes, _, err := pkg.MMapFile(*flagInput)
	if err != nil {
		panic(fmt.Errorf("mmap file: %w", err))
	}

	log.Printf("initializing rng from seed: '%d'", *flagSeed)
	rng := rand.New(rand.NewSource(*flagSeed))

	log.Print("parsing csv from file")
	csvReader := csv.NewReader(bytes.NewReader(inputBytes))
	csvReader.Comma = ';'
	csvReader.Comment = '#'
	csvReader.FieldsPerRecord = 2
	entries, err := csvReader.ReadAll()
	if err != nil {
		panic(fmt.Errorf("csv read all: %w", err))
	}
	since_tReadFile = time.Since(tReadFile)

	tBaseStations := time.Now()
	useCities := int(*flagN) / 10
	if useCities > 0 && useCities < len(entries) {
		entries = entries[:useCities]
	}

	log.Print("generating base stations: ", *flagN)
	stationMap := make(StationMap, len(entries))
	for _, entry := range entries {
		name := entry[0]
		value, _ := strconv.ParseFloat(entry[1], 64)
		if v, ok := stationMap[name]; ok {
			v.Target = (int(value*10) + v.Target) / 2
			continue
		}

		stationMap[name] = &Station{Target: int(value * 10)}
	}
	since_tBaseStations = time.Since(tBaseStations)

	tSort := time.Now()
	cities := maps.Keys(stationMap)
	sort.Strings(cities)
	since_tSort = time.Since(tSort)

	tBulk := time.Now()
	tBulkPrep := time.Now()
	var lines []string = make([]string, 0, *flagN)
	var nextPrint, printIncrement = 0, *flagN / 100
	reportGenProgress := func() {
		if len(lines) >= nextPrint {
			nextPrint += int(printIncrement)
			fmt.Print("\rgeneration progress: ", len(lines))
		}
	}
	since_tBulkPrep = time.Since(tBulkPrep)

	// initial 3
	tBulkInit := time.Now()
	log.Print("generating initial min/mean/max")
	for _, city := range cities {
		station := stationMap[city]
		min, max := station.MinMax()
		vs := []int{min, station.Target, max}
		for _, v := range vs {
			AddValue(station, v)
			lines = append(lines, fmt.Sprintf("%s;%s", city, pkg.PrintIndec(v)))
		}
	}
	since_tBulkInit = time.Since(tBulkInit)

	// bulk
	tBulkBulk := time.Now()
	avgStationCount := int(*flagN) / len(cities)
	bulkSize := (*flagBulk*avgStationCount)/100 - 3
	log.Printf("generating initial bulk: bulk=%d avgCount=%d", bulkSize, avgStationCount)
	if bulkSize > 0 {
		for _, city := range cities {
			if len(lines) >= int(*flagN) {
				break
			}
			station := stationMap[city]

			for i := 0; i < bulkSize; i++ {
				v := station.NaiveValue(rng)
				AddValue(station, v)
				lines = append(lines, fmt.Sprintf("%s;%s", city, pkg.PrintIndec(v)))
				reportGenProgress()

			}
		}
		fmt.Println("")
	}
	since_tBulkBulk = time.Since(tBulkBulk)
	since_tBulk = time.Since(tBulk)

	tErrAvg := time.Now()
	log.Printf("fixing avg errors from %d", len(lines))
	for _, city := range cities {
		station := stationMap[city]
		if station.ValCount == 0 {
			continue
		}
		avgError := station.Target - station.ValSum/station.ValCount
		if avgError == 0 {
			continue
		}

		for avgError != 0 {
			var sign int = 1
			if avgError < 0 {
				sign = -1
			}
			v := station.BiasedValue(rng, sign, avgError)
			AddValue(station, v)
			lines = append(lines, fmt.Sprintf("%s;%s", city, pkg.PrintIndec(v)))
			reportGenProgress()
			avgError = station.Target - station.ValSum/station.ValCount
		}
	}
	fmt.Println("")
	since_tErrAvg = time.Since(tErrAvg)

	tErrSum := time.Now()
	log.Printf("improving sum errors from %d", len(lines))
	const MaxError = SPREAD
	for _, city := range cities {
		if len(lines) >= int(*flagN) {
			break
		}
		station := stationMap[city]
		sumError := station.Target*station.ValCount - station.ValSum
		var sign int = 1
		if sumError < 0 {
			sign = -1
		}

		if sign*sumError <= MaxError {
			continue
		}

		for sign*sumError > MaxError {
			var sign int = 1
			if sumError < 0 {
				sign = -1
			}
			v := station.BiasedValue(rng, sign, sumError)
			AddValue(station, v)
			lines = append(lines, fmt.Sprintf("%s;%s", city, pkg.PrintIndec(v)))
			reportGenProgress()
			sumError = station.Target*station.ValCount - station.ValSum
			if len(lines) >= int(*flagN) {
				break
			}
		}
		if len(lines) >= int(*flagN) {
			break
		}
	}
	fmt.Println("")
	since_tErrSum = time.Since(tErrSum)

	tFill := time.Now()
	log.Printf("filling up from %d to %d", len(lines), *flagN)
	for len(lines) < int(*flagN) {
		city := cities[rng.Intn(len(cities))]
		station := stationMap[city]
		AddValue(station, station.Target)
		lines = append(lines, fmt.Sprintf("%s;%s", city, pkg.PrintIndec(station.Target)))
		reportGenProgress()
	}
	fmt.Println("")
	since_tFill = time.Since(tFill)

	tWriteCheck := time.Now()
	log.Printf("creating check file '%s'", *flagCheck)
	checkFile, err := os.OpenFile(*flagCheck, os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(fmt.Errorf("open check file '%s': %w", *flagCheck, err))
	}

	log.Print("filling check file")
	for i, city := range cities {
		station := stationMap[city]
		if station.ValCount == 0 {
			continue
		}
		min, max := station.MinMax()
		line := fmt.Sprintf("%s=%s/%s/%s\n", city, pkg.PrintIndec(min), pkg.PrintIndec(station.Target), pkg.PrintIndec(max))
		checkFile.WriteString(line)
		if i%(len(cities)/10) == 0 {
			fmt.Printf("\rdone: %d0%%", i/(len(cities)/10))
		}
	}
	checkFile.Close()
	fmt.Println("")
	since_tWriteCheck = time.Since(tWriteCheck)

	if *flagFile != "" {
		tDerange := time.Now()
		log.Print("randomizing lines")
		randomIndices := derange(rng, int(*flagN))
		var sb strings.Builder
		for j, i := range randomIndices {
			line := lines[i]
			sb.WriteString(line)
			sb.WriteByte('\n')
			if j%(int(*flagN/100)) == 0 {
				fmt.Printf("\rdone: %d%%", j/(int(*flagN/100)))
			}
		}
		since_tDerange = time.Since(tDerange)

		tOutput := time.Now()
		log.Printf("creating output file '%s'", *flagFile)
		outputFile, err := os.OpenFile(*flagFile, os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			panic(fmt.Errorf("open output file '%s': %w", *flagFile, err))
		}

		log.Print("writing file")
		n, err := outputFile.WriteString(sb.String())
		if err != nil {
			panic(fmt.Errorf("write output file: %w", err))
		}
		outputFile.Close()
		log.Printf("Test data build complete. wrote %d bytes in %d lines\n", n, len(lines))
		since_tOutput = time.Since(tOutput)
	}

	log.Printf(`
[ ReadFile: %v
[ BaseStations: %v
[ Sort: %v
[ Bulk: %v
  > BulkPrep: %v
  > BulkInit: %v
  > BulkBulk: %v
[ ErrAvg: %v
[ ErrSum: %v
[ Fill: %v
[ WriteCheck: %v
[ Derange: %v
[ Output: %v
= Total: %v
			 `,
		since_tReadFile,
		since_tBaseStations,
		since_tSort,
		since_tBulk,
		since_tBulkPrep,
		since_tBulkInit,
		since_tBulkBulk,
		since_tErrAvg,
		since_tErrSum,
		since_tFill,
		since_tWriteCheck,
		since_tDerange,
		since_tOutput,
		time.Since(tTotal),
	)
}

func derange(rng *rand.Rand, n int) []int {
	arr := make([]int, n)
	for i := range n {
		arr[i] = i
	}

	for i := n - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		for j == i {
			j = rng.Intn(i + 1)
		}
		arr[i], arr[j] = arr[j], arr[i]
	}

	for i, val := range arr {
		if i == val {
			if i == 0 {
				arr[i], arr[i+1] = arr[i+1], arr[i]
			} else {
				arr[i], arr[i-1] = arr[i-1], arr[i]
			}
		}
	}

	return arr
}
