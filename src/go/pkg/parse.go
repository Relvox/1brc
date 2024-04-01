package pkg

import (
	"bytes"
)

func SplitParse(line []byte) (key []byte, val int) {
	semiColonIndex := bytes.IndexByte(line, ';')
	key = line[:semiColonIndex]
	val = ParseIndec(line[semiColonIndex+1:])
	return
}

type KVP struct {
	Key   []byte
	Value int
}
