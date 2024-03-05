package unemployment

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"p3-gcp/cazip"
	"sync"
	"time"
)

// Define global variables for transportation package
var (
	logger    *log.Logger
	mu        sync.Mutex
	caLookup  cazip.CALookupList
	zipLookup cazip.ZipLookupList
)

func init() {
	// Open or create a log file
	currentTime := time.Now().Format("2006-01-02_15-04-05")
	logFilename := fmt.Sprintf("logfiles/unemployment_%s.log", currentTime)
	file, err := os.OpenFile(logFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}

	// Create a logger that writes to the file
	logger = log.New(file, "", log.LstdFlags)
}

func writeToLog(format string, args ...interface{}) {
	// Use a mutex to ensure safe concurrent access to the log file
	mu.Lock()
	defer mu.Unlock()
	message := fmt.Sprintf(format, args...)
	logger.Println(message)
}

// Generator function turns API response into stream of taxiTrip structs
func generator(done <-chan interface{}, unempList []unemployment) <-chan unemployment {
	unempStream := make(chan unemployment)
	go func() {
		defer close(unempStream)
		for _, record := range unempList {
			select {
			case <-done:
				return
			case unempStream <- record:
			}
		}
	}()
	return unempStream
}

func BuildUnemploymentTable() error {

	caLookup = cazip.GetCaLookupList()
	zipLookup = cazip.GetZipLookupList()

	// This dataset contains only 77 records so only need a single API call

	unempURL := "https://data.cityofchicago.org/resource/iqnk-2tcu.json"

	fmt.Println("Starting Build Unemployment")
	writeToLog("Starting Build Unemployment")

	// Make sure that the unemployment table is created
	err := createUnemploymentTable()
	if err != nil {
		return fmt.Errorf("Error creating unemployment table: %v", err)
	}

	done := make(chan interface{})
	defer close(done)

	var unempRecords []unemployment

	response := getUnemployment(unempURL)
	if response.Error != nil {
		fmt.Println("Error:", response.Error)
		writeToLog("Error: %s", response.Error)
	}

	b, _ := io.ReadAll(response.Response.Body)
	json.Unmarshal(b, &unempRecords)
	for i := range unempRecords {
		unempRecords[i].URL = unempURL
	}

	writeToLog("Response: %s", response.Response.Status)

	fmt.Println("Number of Unemployment records: ", len(unempRecords))
	writeToLog("Completed API pull.")
	writeToLog("Number of Unemployment records: %d", len(unempRecords))

	fmt.Println("Starting pipeline to clean unemplyment data and write to PostgreSQL.")
	writeToLog("Starting pipeline to clean data and write to PostgreSQL.")
	unempStream := generator(done, unempRecords)
	errCount := 0
	countWritten := 0
	for cu := range cleanUnemployment(done, unempStream) {
		written, err := addUnempRecord(cu)
		if err != nil {
			fmt.Println(err)
			writeToLog("Error writing %s?community_area=%s record to database.", cu.API, cu.ComArea)
			errCount++
		}
		if written {
			countWritten++
		}
		if errCount > 10 {
			writeToLog("Too many errors writing to database. Stopping.")
			return fmt.Errorf("Error- too many errors writing to unemployment database. Stopping.")
		}
	}

	fmt.Println("Finished Build Unemployment: ", countWritten)
	writeToLog("Finished Build Unemployment: %v", countWritten)

	return nil
}
