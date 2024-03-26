.PHONY: 10 50 100 all clean bench report flame

100: main.exe 
	main.exe -n 1_000_000_000 -prof p.prof 2>log

50: main.exe
	main.exe -n 500_000_000 2>log050

10: main.exe
	main.exe -n 100_000_000 2>log010

all: 10 50 100
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