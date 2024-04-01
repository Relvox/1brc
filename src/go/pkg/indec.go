package pkg

import (
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
