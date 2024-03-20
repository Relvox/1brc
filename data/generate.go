package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func checkArgs() int {
	if len(os.Args) != 2 {
		fmt.Println("Usage: create_measurements <positive integer number of records to create>")
		fmt.Println("       You can use underscore notation for large number of records.")
		fmt.Println("       For example:  1_000_000_000 for one billion")
		os.Exit(1)
	}
	numRecords, err := strconv.Atoi(strings.ReplaceAll(os.Args[1], "_", ""))
	if err != nil || numRecords <= 0 {
		fmt.Println("Error: Argument must be a positive integer.")
		os.Exit(1)
	}
	return numRecords
}

func buildWeatherStationNameList() []string {
	file, err := os.Open("weather_stations.csv")
	if err != nil {
		log.Fatalf("Failed to open weather stations file: %v", err)
	}
	defer file.Close()

	var stationNames []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, ";")
		if len(fields) > 0 {
			stationNames = append(stationNames, fields[0])
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
	return unique(stationNames)
}

func unique(strSlice []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range strSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func convertBytes(num float64) string {
	units := []string{"bytes", "KiB", "MiB", "GiB"}
	for _, unit := range units {
		if num < 1024.0 {
			return fmt.Sprintf("%3.1f %s", num, unit)
		}
		num /= 1024.0
	}
	return fmt.Sprintf("%.1f %s", num, "TiB")
}

func formatElapsedTime(seconds int) string {
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	seconds = (seconds % 3600) % 60
	return fmt.Sprintf("%02d hours %02d minutes %02d seconds", hours, minutes, seconds)
}

func estimateFileSize(weatherStationNames []string, numRowsToCreate int) string {
	var totalNameBytes int
	for _, s := range weatherStationNames {
		totalNameBytes += len(s)
	}
	avgNameBytes := float64(totalNameBytes) / float64(len(weatherStationNames))
	avgTempBytes := 4.400200100050025
	avgLineLength := avgNameBytes + avgTempBytes + 2
	humanFileSize := convertBytes(float64(numRowsToCreate) * avgLineLength)
	return fmt.Sprintf("Estimated max file size is:  %s.", humanFileSize)
}

func buildTestData(weatherStationNames []string, numRowsToCreate int) {
	startTime := time.Now()
	coldestTemp := -99.9
	hottestTemp := 99.9
	rand.Seed(time.Now().UnixNano())
	file, err := os.Create("measurements.txt")
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer file.Close()

	for i := 0; i < numRowsToCreate; i++ {
		station := weatherStationNames[rand.Intn(len(weatherStationNames))]
		temp := rand.Float64()*(hottestTemp-coldestTemp) + coldestTemp
		_, err := file.WriteString(fmt.Sprintf("%s;%.1f\n", station, temp))
		if err != nil {
			log.Fatalf("Failed to write to output file: %v", err)
		}
	}

	elapsedTime := time.Since(startTime).Seconds()
	fileInfo, err := os.Stat("measurements.txt")
	if err != nil {
		log.Fatalf("Failed to get file info: %v", err)
	}
	humanFileSize := convertBytes(float64(fileInfo.Size()))
	fmt.Printf("Test data successfully written to measurements.txt\n")
	fmt.Printf("Actual file size: %s\n", humanFileSize)
	fmt.Printf("Elapsed time: %s\n", formatElapsedTime(int(elapsedTime)))
}

func main() {
	numRowsToCreate := checkArgs()
	weatherStationNames := buildWeatherStationNameList()
	fmt.Println(estimateFileSize(weatherStationNames, numRowsToCreate))
	buildTestData(weatherStationNames, numRowsToCreate)
	fmt.Println("Test data build complete.")
}
