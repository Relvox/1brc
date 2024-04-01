package pkg

import (
	"bytes"
	"hash/fnv"
)

type HashKey = uint32

func Hash(s []byte) HashKey {
	h := fnv.New32a()
	h.Write(s)
	res := h.Sum32()
	return res
}

func SplitParse(line []byte) (key []byte, val int) {
	semiColonIndex := bytes.IndexByte(line, ';')
	key = line[:semiColonIndex]
	val = ParseIndec(line[semiColonIndex+1:])
	return
}

type HKV struct {
	Hash  HashKey
	Key   []byte
	Value int
}