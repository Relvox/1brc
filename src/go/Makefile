.PHONY: 1 5 10 20 50 100 all clean bench report flame

100: main.exe 
	main.exe -n 1_000_000_000 -prof p.prof 2>log

50: main.exe
	main.exe -n 500_000_000 2>log050

20: main.exe
	main.exe -n 200_000_000 2>log020

10: main.exe
	main.exe -n 100_000_000 2>log010

5: main.exe
	main.exe -n 50_000_000 2>log005

1: main.exe
	main.exe -n 10_000_000 2>log001

all: 1 5 10 20 50 100
	echo done.

main.exe: clean
	echo Building ... ; \
	go build main.go

clean:
	- rm main.exe

bench:
	go.exe test -benchmem -run=^$$ -bench '^Benchmark' .

flame:
	go tool pprof --trim=false -raw -output=p.txt p.prof && \
	./stackcollapse-go.pl p.txt | ./flamegraph.pl > flame.svg && \
	start flame.svg

report: flame
	go tool pprof --trim=false -web ./p.prof