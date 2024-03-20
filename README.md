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
      2024/03/21 11:52:42 = Scanning Took: 3m4.381126s
      2024/03/21 11:52:42 = Sorting Took: 6.9992ms
      2024/03/21 11:52:42 = Printing Took: 2.800709s
      2024/03/21 11:52:42 = Total Took: 3m7.3919923s
      ```
      > building saved 2s. not worth it at this point.

## Research

1. Refresher on how to read file line by line :P

# Ideas

1. Generation is random is not really verifiable.
2. Generation is slow?