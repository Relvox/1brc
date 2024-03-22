package main_test

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func Test(t *testing.T) {
	arr := []int{}
	for i := 0; i < 10000000; i++ {
		arr = append(arr, i%1024)
	}

	t0 := time.Now()
	sort.Ints(arr)
	since_t0 := time.Since(t0)
	t1 := time.Now()
	sort.Ints(arr)
	since_t1 := time.Since(t1)
	t2 := time.Now()
	sort.Ints(arr)
	since_t2 := time.Since(t2)

	fmt.Println(since_t0)
	fmt.Println(since_t1)
	fmt.Println(since_t2)
}
