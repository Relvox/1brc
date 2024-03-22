package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"time"
)

const (
	INPUT_BYTES = 15_884_020_369
	// INPUT_TEST_BYTES = 386_842_110
	BUF = 1024000
)

func main() {
	flagProf := flag.String("prof", "", "write cpu profile to file")
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
	var sb strings.Builder
	minBuf, count := 0, 0
	var sumTime time.Duration
	minTime := time.Second * 10
	// for BUF := 490_000; BUF <= 520_000; BUF += 1 {
	// fmt.Print(BUF, " ")
	// file, err := os.Open("../../../data/test")
	file, err := os.Open("../../../data/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	t1 := time.Now()
	buf := make([]byte, BUF)
	var off, i int64
	var n int

	bufs := make([][]byte, 0, INPUT_BYTES/BUF+1)
	nextPrint := 0
	for i = int64(0); ; i += int64(n) {
		for i >= int64(nextPrint) {
			fmt.Print("*")
			nextPrint += INPUT_BYTES / 25
		}

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
		bufs = append(bufs, bytes.Clone(buf))
		if err == io.EOF {
			break
		}
	}

	_ = bufs
	// fmt.Println("")
	since_t1 := time.Since(t1)
	if since_t1 < minTime {
		minTime = since_t1
		minBuf = BUF
		fmt.Println("New Min", minBuf, minTime)
	}
	sumTime += since_t1
	count++
	fmt.Fprintf(&sb, "%d, %d, %d, %d, %d,\n",
		BUF, since_t1, int(sumTime)/count, minBuf, minTime)
	// }
	os.WriteFile("csv.csv", []byte(sb.String()), 0644)
}
