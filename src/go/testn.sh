#!/bin/sh

N=$1

if [ -z "$N" ] || ! [ "$N" -gt 0 ] 2>/dev/null; then
    echo "Usage: $0 <N>"
    echo "<N> must be a positive integer."
    exit 1
fi


make clean 
make gen ARGS_GEN="-file './test/${N}0.txt' -check './test/${N}0.chk' -n ${N}0 -bulk 80"
make main ARGS_MAIN="-n ${N}0 -max ${N}0 -file ./test/${N}0.txt" AND="1>./test/${N}0.ans"

# Compare files
cmp ./test/${N}0.chk ./test/${N}0.ans
