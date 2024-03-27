.PHONY: clean main gen build report flame compare

BIN=./bin
PROF=./prof
DATA=../../data
EXT=../external

SRC_MAIN=./cmd
SRC_GEN=./cmd/gen

EXEC_MAIN=$(BIN)/main.exe
EXEC_GEN=$(BIN)/gen.exe

FILE_OUT=$(DATA)/measurements.ans
FILE_MAIN=$(DATA)/measurements.txt
FILE_CHECK=$(DATA)/measurements.chk
FILE_GEN_INPUT=$(DATA)/weather_stations.csv

$(FILE_CHECK): $(EXEC_GEN)
	make gen ARGS_GEN='-file=""'

$(FILE_OUT): $(EXEC_MAIN) main


AND=""
ARGS_GEN=""
ARGS_MAIN=""

# Simplified build rules
$(EXEC_MAIN):
	go build -o $@ $(SRC_MAIN)/main.go

$(EXEC_GEN):
	go build -o $@ $(SRC_GEN)/main.go

# Main targets
main: $(EXEC_MAIN) 
	$< -prof=$(PROF)/main.prof $(ARGS_MAIN) $(AND) 1>$(FILE_OUT)

gen: $(EXEC_GEN) 
	$< -prof=$(PROF)/gen.prof $(ARGS_GEN) $(AND)

build: $(EXEC_MAIN) $(EXEC_GEN)

clean:
	- rm -f $(BIN)/*

# Profiling and reports
flame.%: $(PROF)/%.prof
	go tool pprof --trim=false -raw -output=$(PROF)/$*.raw $< && \
	$(EXT)/stackcollapse-go.pl $(PROF)/$*.raw | $(EXT)/flamegraph.pl > $(PROF)/$*.flame.svg && \
	start $(PROF)/$*.flame.svg

report.%: flame.%
	go tool pprof --trim=false -svg $(PROF)/$*.prof > $*.svg && \
	start $(PROF)/$*.svg


flame: flame.gen flame.main
report: report.gen report.main

compare: $(FILE_CHECK) $(FILE_OUT) 
	cmp $^
