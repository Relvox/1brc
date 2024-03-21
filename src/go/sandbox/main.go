package main

import (
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
// BUF = 1024*1024/4 // 262144
// BUF = 255000
)

func main() {
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
	var sb strings.Builder

	// ~ 512000
	for BUF := 128000; BUF < 768000; BUF += 1024 {
		file, err := os.Open("../../../data/measurements.txt")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		t1 := time.Now()
		buf := make([]byte, BUF)
		var off int64
		var n int
		for i := int64(0); ; i += int64(n) {
			if i >= *flagN {
				break
			}

			n, err = file.ReadAt(buf, off)

			if n < BUF || err == io.EOF {
				break
			}
		}

		since_t1 := time.Since(t1)
		fmt.Fprintf(&sb, "%d, %v,\n",BUF, since_t1)
	}
	fmt.Println(sb.String())
}
