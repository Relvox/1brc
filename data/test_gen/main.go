package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	var sb strings.Builder
	for i := range 50_000_000 {
		fmt.Fprintf(&sb, "name%1dname;%1d%1d.%1d\n", i%10, i%19-9, i%10, i%5)
	}
	os.WriteFile("../test2", []byte(sb.String()), 0644)
}
