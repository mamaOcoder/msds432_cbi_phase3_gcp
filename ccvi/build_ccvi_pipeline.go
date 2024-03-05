package ccvi

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
	logFilename := fmt.Sprintf("logfiles/ccvi_%s.log", currentTime)
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

// Generator function turns API response into stream of ccvi structs
func generator(done <-chan interface{}, ccviList []ccvi) <-chan ccvi {
	ccviStream := make(chan ccvi)
	go func() {
		defer close(ccviStream)
		for _, ccvi := range ccviList {
			select {
			case <-done:
				return
			case ccviStream <- ccvi:
			}
		}
	}()
	return ccviStream
}

func BuildCcviTable() error {

	//There is only 135 records in the dataset so only need a single API call
	//
	ccviURL := "https://data.cityofchicago.org/resource/xhc6-88s9.json"

	fmt.Println("Starting Build CCVI")
	writeToLog("Starting Build CCVI")

	// Make sure that the taxi table is created
	err := createCcviTable()
	if err != nil {
		return fmt.Errorf("Error creating CCVI table: %v", err)
	}

	done := make(chan interface{})
	defer close(done)

	var ccviRecords []ccvi

	response := getCCVI(ccviURL)
	if response.Error != nil {
		fmt.Println("Error:", response.Error)
		writeToLog("Error: %s", response.Error)
	}

	b, _ := io.ReadAll(response.Response.Body)
	json.Unmarshal(b, &ccviRecords)
	for i := range ccviRecords {
		ccviRecords[i].URL = ccviURL
	}
	writeToLog("Response: %s", response.Response.Status)

	fmt.Println("Number of CCVI records: ", len(ccviRecords))
	writeToLog("Completed API pull.")
	writeToLog("Number of CCVI records: %d", len(ccviRecords))

	fmt.Println("Starting pipeline to clean CCVI data and write to PostgreSQL.")
	writeToLog("Starting pipeline to clean data and write to PostgreSQL.")
	//Turn the responses into a stream for processing
	ccviStream := generator(done, ccviRecords)
	errCount := 0
	countWritten := 0
	for cc := range cleanCCVI(done, ccviStream) {
		written, err := addCcviRecord(cc)
		if err != nil {
			fmt.Println(err)
			writeToLog("Error writing %s?%s record to database.", cc.API, cc.ID)
			errCount++
		}
		if written {
			countWritten++
		}
		if errCount > 10 {
			writeToLog("Too many errors writing to database. Stopping.")
			return fmt.Errorf("Error- too many errors writing to CCVI database. Stopping.")
		}
	}
	fmt.Println("Finished Build CCVI: ", countWritten)
	writeToLog("Finished Build CCVI: %v", countWritten)

	return nil
}
