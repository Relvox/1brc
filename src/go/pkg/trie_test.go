package pkg_test

import (
	"brc/pkg"
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

func Test_Trie(t *testing.T) {
	root := pkg.MakeRoot()
	root.Insert([]byte("abc"), &pkg.CityData{1, 1, 1, 1})
	fmt.Println(root)
	log.Println(root.Get([]byte("abc")), root.Get([]byte("xyz")), root.Get([]byte("abc")))

	root.Insert([]byte("xyz"), &pkg.CityData{2, 2, 2, 1})
	fmt.Println(root)
	log.Println(root.Get([]byte("xyz")), root.Get([]byte("xyz")), root.Get([]byte("abc")))

	root.Insert([]byte("abcd"), &pkg.CityData{3, 3, 3, 1})
	fmt.Println(root)
	log.Println(root.Get([]byte("abcd")), root.Get([]byte("xyz")), root.Get([]byte("abc")))

	root.Insert([]byte("ayz"), &pkg.CityData{4, 4, 4, 1})
	fmt.Println(root)
	log.Println(root.Get([]byte("ayz")), root.Get([]byte("xyz")), root.Get([]byte("abc")))

	root.Insert([]byte("abz"), &pkg.CityData{5, 5, 5, 1})
	fmt.Println(root)
	log.Println(root.Get([]byte("abz")), root.Get([]byte("xyz")), root.Get([]byte("abc")))

	root.Insert([]byte("abc"), &pkg.CityData{6, 6, 6, 1})
	fmt.Println(root)
	log.Println(root.Get([]byte("abc")), root.Get([]byte("xyz")), root.Get([]byte("abc")))
}

type KV struct {
	Key   []byte
	Value int
}

func Test_Trie2(t *testing.T) {
	file, _ := os.Open("../../../data/measurements.txt")
	buf := make([]byte, 1024*1024*256)
	file.Read(buf)
	batch := make([]pkg.KVP, 0)
	var key []byte
	var val int
	for {
		m := bytes.IndexByte(buf, 10)
		if m < 0 {
			break
		} else {
			key, val = pkg.SplitParse(buf[:m])
		}

		batch = append(batch, pkg.KVP{pkg.Hash(key), key, val})
		if m < 0 {
			break
		}
		buf = buf[m+1:]
	}

	// t.Run("Map1", func(t *testing.T) {
	// 	var v0, v1 int
	// 	t0 := time.Now()
	// 	output := make(map[pkg.HashKey]*pkg.CityData)
	// 	for _, kvp := range batch {
	// 		data, ok := output[kvp.Hash]
	// 		if !ok {
	// 			v0++
	// 			output[kvp.Hash] = &pkg.CityData{
	// 				kvp.Value,
	// 				kvp.Value,
	// 				kvp.Value,
	// 				1,
	// 				kvp.Key,
	// 			}
	// 			continue
	// 		}
	// 		v1++
	// 		data.MergeValue(kvp.Value)
	// 	}
	// 	log.Println(v0, v1, time.Since(t0))
	// })
	t.Run("Trie", func(t *testing.T) {
		foo := map[string]bool{}
		var v0, v1 int
		t0 := time.Now()
		output := pkg.MakeRoot()
		for _, kvp := range batch {
			data := output.Get(kvp.Key)
			if data == nil {
				v0++
				foo[string(kvp.Key)] = true
				output.Insert(kvp.Key, &pkg.CityData{
					Min:   kvp.Value,
					Sum:   kvp.Value,
					Max:   kvp.Value,
					Count: 1,
				})

				continue
			}
			v1++
			data.MergeValue(kvp.Value)
		}
		log.Println(v0, v1, time.Since(t0))
		output.Iter(nil, func(key []byte, value *pkg.CityData) {
			fmt.Println(string(key), value)
		})
	})
}
