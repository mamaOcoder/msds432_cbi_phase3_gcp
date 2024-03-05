package building_permits

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

// Define global variables for building permits package
var (
	logger    *log.Logger
	mu        sync.Mutex
	caLookup  cazip.CALookupList
	zipLookup cazip.ZipLookupList
)

func init() {
	// Open or create a log file
	currentTime := time.Now().Format("2006-01-02_15-04-05")
	logFilename := fmt.Sprintf("logfiles/building_permits_%s.log", currentTime)
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
func generator(done <-chan interface{}, permitsList []buildingPermit) <-chan buildingPermit {
	permitsStream := make(chan buildingPermit)
	go func() {
		defer close(permitsStream)
		for _, permit := range permitsList {
			select {
			case <-done:
				return
			case permitsStream <- permit:
			}
		}
	}()
	return permitsStream
}

func BuildPermitsTable() error {
	caLookup = cazip.GetCaLookupList()
	zipLookup = cazip.GetZipLookupList()

	recordLimit := 10000

	permitURLs := []string{"https://data.cityofchicago.org/resource/ydr8-5enu.json"}

	fmt.Println("Starting Build Permits")
	writeToLog("Starting Build Permits")

	// Make sure that the permits table is created
	/*err := createPermitTable()
	if err != nil {
		return fmt.Errorf("Error creating building_permits table: %v", err)
	}*/

	queryURLs := common.BuildUrls(permitURLs, recordLimit)

	done := make(chan interface{})
	defer close(done)

	const maxWorkers = 10 // Limiting simultaneous API requests
	semaphore := make(chan struct{}, maxWorkers)

	var buildingPermits []buildingPermit
	for _, url := range queryURLs {
		semaphore <- struct{}{}
		go func(url string) {
			defer func() { <-semaphore }()
			for response := range getPermits(done, url) {
				if response.Error != nil {
					fmt.Println("Error:", response.Error)
					writeToLog("Error: %s", response.Error)
				} else {
					var permits []buildingPermit
					b, _ := io.ReadAll(response.Response.Body)
					json.Unmarshal(b, &permits)
					for i := range permits {
						permits[i].URL = url
					}
					buildingPermits = append(buildingPermits, permits...)
					writeToLog("Response: %s", response.Response.Status)
				}
			}
		}(url)
	}

	// Ensure all goroutines finish before exiting BuildTaxiTable
	for i := 0; i < maxWorkers; i++ {
		semaphore <- struct{}{}
	}

	fmt.Println("Number of building permit records: ", len(buildingPermits))
	writeToLog("Completed API pull.")
	writeToLog("Number of building permit records: %d", len(buildingPermits))

	fmt.Println("Starting pipeline to clean building permit data and write to PostgreSQL.")
	writeToLog("Starting pipeline to clean data and write to PostgreSQL.")
	//Turn the responses into a stream for processing
	permitStream := generator(done, buildingPermits)
	/*errCount := 0
	//countpermit := 0
	for cp := range cleanPermit(done, permitStream) {
		err := addPermitRecord(cp)
		if err != nil {
			fmt.Println(err)
			writeToLog("Error writing %s?%s permit to database.", cp.API, cp.ID)
			errCount++
		}
		if errCount > 10 {
			writeToLog("Too many errors writing to database. Stopping.")
			return fmt.Errorf("Error - too many errors writing to building permits database. Stopping.")
		}
	}*/

	for range cleanPermit(done, permitStream) {
	}

	fmt.Println("Finished Build Permits")
	writeToLog("Finished Build Permits")

	return nil
}
