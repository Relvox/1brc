# 1BRC

## Primary Challenge

* Given a data file [like this](./data/measurements.txt) containing 1_000_000_000 rows, process the file and print the output as fast as possible.
* Each row (line) represents a single measurement of temperature in a city.
  * Format: `<Name>;<Temperature>\n`
    * `<Name>` may contain spaces, punctuations, or non-ascii chars.
    * `<Temperature>` is a decimal number $\in [-99.9, 99.9]$. (Regex: `-?\d?\d\.\d?`)
* Aggregate the measurements per city and output the following values, sorted by city name:
  * `<Name>=<Min>/<Mean>/<Max>\n`


## Secondary Challenge

* Given a source file [like this](./data/weather_stations.csv) containing a ~30k rows, process the file and generate a `measurements.txt` file for the main challenge and a `measurements.chk` file used to validate the main answer.
* Each row is of the format: `<Name>;<Temperature>\n` 
  * `<Name>` may contain spaces, punctuations, or non-ascii chars.
  * `<Temperature>` is a float $\in [-99.9, 99.9]$.

## Progress

* Primary:
  ```
  [ Read: 33.1305ms
  [ Parse: 8.5803861s
    > Wait: 1.4847199s
  [ MapData: 8.5803861s
    > Wait: 0s
  ? Merge Maps: 8.5823856s
  ? Sorting: 9.9998ms
  ? Print Prep: 25.0004ms
  ? Printing: 0s // redirected stdout to file
  = Total: 8.6242789s
  ```
* Secondary:
  ```
  [ ReadFile: 8.7091ms
  [ BaseStations: 7.9366ms
  [ Sort: 10.2092ms
  [ Bulk: 3m21.402899s
    > BulkPrep: 17.5183ms
    > BulkInit: 109.1154ms
    > BulkBulk: 3m21.2762653s
  [ ErrAvg: 1.2092587s
  [ ErrSum: 703.9353ms
  [ Fill: 1m6.7723508s
  [ WriteTest: 107.5084ms
  [ Derange: 0s // skipped measurements.txt this time
  [ Output: 0s // skipped measurements.txt this time
  = Total: 4m30.2301066s
  ```