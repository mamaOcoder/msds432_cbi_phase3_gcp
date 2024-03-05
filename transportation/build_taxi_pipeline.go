package transportation

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"p3-gcp/cazip"
	"p3-gcp/common"
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
	logFilename := fmt.Sprintf("logfiles/transportation_%s.log", currentTime)
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
func generator(done <-chan interface{}, taxiTripList []taxiTrip) <-chan taxiTrip {
	taxiStream := make(chan taxiTrip)
	go func() {
		defer close(taxiStream)
		for _, trip := range taxiTripList {
			select {
			case <-done:
				return
			case taxiStream <- trip:
			}
		}
	}()
	return taxiStream
}

func BuildTaxiTable() error {

	//Load lookup tables for use when cleaning
	caLookup = cazip.GetCaLookupList()
	zipLookup = cazip.GetZipLookupList()

	recordLimit := 2500

	taxiURLs := []string{"https://data.cityofchicago.org/resource/ajtu-isnz.json",
		"https://data.cityofchicago.org/resource/n26f-ihde.json"}

	fmt.Println("Starting Build Taxi")
	writeToLog("Starting Build Taxi")

	// Make sure that the taxi table is created
	err := createTaxiTable()
	if err != nil {
		return fmt.Errorf("Error creating taxi table: %v", err)
	}

	queryURLs := common.BuildUrls(taxiURLs, recordLimit)

	done := make(chan interface{})
	defer close(done)

	const maxWorkers = 10 // Limiting simultaneous API requests
	semaphore := make(chan struct{}, maxWorkers)

	var taxiTrips []taxiTrip
	for _, url := range queryURLs {
		semaphore <- struct{}{}
		go func(url string) {
			defer func() { <-semaphore }()
			for response := range getTaxi(done, url) {
				if response.Error != nil {
					fmt.Println("Error:", response.Error)
					writeToLog("Error: %s", response.Error)
				} else {
					var trips []taxiTrip
					b, _ := io.ReadAll(response.Response.Body)
					json.Unmarshal(b, &trips)
					for i := range trips {
						trips[i].URL = url
					}
					taxiTrips = append(taxiTrips, trips...)
					writeToLog("Response: %s", response.Response.Status)
				}
			}
		}(url)
	}

	// Ensure all goroutines finish before exiting BuildTaxiTable
	for i := 0; i < maxWorkers; i++ {
		semaphore <- struct{}{}
	}

	fmt.Println("Number of taxi trip records: ", len(taxiTrips))
	writeToLog("Completed API pull.")
	writeToLog("Number of taxi trip records: %d", len(taxiTrips))

	fmt.Println("Starting pipeline to clean taxi data and write to PostgreSQL.")
	writeToLog("Starting pipeline to clean data and write to PostgreSQL.")
	//Turn the responses into a stream for processing
	taxiStream := generator(done, taxiTrips)
	errCount := 0
	countWritten := 0
	for ct := range cleanTaxi(done, taxiStream) {
		written, err := addTaxiTrip(ct)
		if err != nil {
			fmt.Println(err)
			writeToLog("Error writing %s?%s trip to database.", ct.API, ct.TripID)
			errCount++
		}
		if written {
			countWritten++
		}
		if errCount > 10 {
			writeToLog("Too many errors writing to database. Stopping.")
			return fmt.Errorf("Error- too many errors writing to taxi database. Stopping.")
		}
	}

	fmt.Println("Finished Build Taxi: ", countWritten)
	writeToLog("Finished Build Taxi: %v", countWritten)

	return nil
}
