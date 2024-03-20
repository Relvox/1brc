.PHONY: 1 5 10 20 50 100 all clean bench

100: main.exe 
	./main.exe -n 1_000_000_000 -cprof log.prof 2>log

50: main.exe
	./main.exe -n 500_000_000 -cprof log050.prof 2>log050

20: main.exe
	./main.exe -n 200_000_000 -cprof log020.prof 2>log020

10: main.exe
	./main.exe -n 100_000_000 -cprof log010.prof 2>log010

5: main.exe
	./main.exe -n 50_000_000 -cprof log005.prof 2>log005

1: main.exe
	./main.exe -n 10_000_000 -cprof log001.prof 2>log001

all: clean 1 5 10 20 50 100
	echo done.

main.exe:
	echo Building ... ; \
	go build main.go

clean:
	- rm main.exe

bench:
	go.exe test -benchmem -run=^$$ -bench '^Benchmark' .