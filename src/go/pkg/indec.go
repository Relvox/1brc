package pkg

import (
	"bytes"
	"fmt"
)

func PrintIndec(i int) string {
	var sign string
	if i < 0 {
		sign = "-"
		i = -i
	}
	return fmt.Sprint(sign, i/10, ".", i%10)
}

func ParseIndec(bs []byte) int {
	var result, startValIndex int
	dotIndex := bytes.IndexByte(bs, '.')
	neg := bs[0] == '-'
	if neg {
		startValIndex++
	}

	for i := startValIndex; i < dotIndex; i++ {
		result = result*10 + int(bs[i]-'0')
	}

	if dotIndex+1 < len(bs) {
		result = result*10 + int(bs[dotIndex+1]-'0')
	}

	if neg {
		result = -result
	}

	return result
}
