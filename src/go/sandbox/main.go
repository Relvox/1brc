package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

const (
	INPUT_BYTES = 15884020369
	BUF         = 1024000
)

func main() {
	flagProf := flag.String("prof", "", "write cpu profile to file")

	flag.Parse()

	if *flagProf != "" {
		f, err := os.Create(*flagProf)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	for BUF := 10; BUF <= 1024; BUF += 2{
		fmt.Print(BUF*1024, " ")
		file, err := os.Open("../../../data/measurements.txt")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		t1 := time.Now()
		buf := make([]byte, BUF*1024)
		var off, i int64
		var n int

		nextPrint := 0
		for i = int64(0); ; i += int64(n) {
			if i >= int64(nextPrint) {
				fmt.Print("*")
				nextPrint += 100_000_000
			}
			n, err = file.ReadAt(buf, off)
			for n = n - 1; n >= 0; n-- {
				if buf[n] == '\n' {
					break
				}
			}
			off += int64(n) + 1
			if err == io.EOF {
				break
			}
		}
		if off != INPUT_BYTES {
			panic(i)
		}

		since_t1 := time.Since(t1)
		fmt.Println("", since_t1)
	}
}
