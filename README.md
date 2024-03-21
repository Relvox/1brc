# 1brc log

1. Make GPT4 Translate file generation code to Go.
2. Run to generate 1B measurements, take a v.long time.
   1. Took 48 mins
   2. Generate 15.5g wincount (14.7Gb)
3. Start with naive go attempt:
   1. read line by line, aggregate to map[string]data
   2. data= min, max, sum, count
   3. with fmt.Println, printing caps at ~2.7s
   4. On my win machine with ~3 days uptime and lots of chrome/vscode open
      ```
      Processor: 12th Gen Intel(R) Core(TM) i7-12700KF (20 CPUs), ~3.6GHz
      Memory: 65,536MB RAM
      HDD: Samsung SSD 980 PRO 1TB [Sequential Read: Up to 7,000 MB/s]
      ```
   5. Result:
      ```
      = Scanning Took: 3m4.381126s
      = Sorting Took: 6.9992ms
      = Printing Took: 2.800709s
      = Total Took: 3m7.3919923s
      ```
      > building saved 2s. not worth it at this point.
4. low effort: try to use stringsBuilder for printing
   > does it really help? is it within fluctuation range?  
   > What if we try changing the map to pointers? looks better :)
   ```
   = Scanning Took: 3m2.9752375s
     - Reading: 35.2557324s
     - Processing: 2m24.3343125s
   = Sorting Took: 8.7878ms
   = Printing Took: 1.7957342s
   = Total Took: 3m4.7802828s
   ```
5. Custom Split-Parsing and Int cheat

## Research

1. Refresher on how to read file line by line :P

# Ideas

1. Generation is random is not really verifiable.
2. Generation is slow?