package covid

import (
	"bufio"
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

// There are some zip codes in this dataset that are outside of the City of Chicago Boundaries- Community Areas Dataset
// The coordinates provided in the Covid19 dataset are a point within the zip code.
// I will record the results from doing Reverse Lookup here to reduce the number of calls to the Google API
type zipCaCovidLookup struct {
	ZipCode     string    `json:"zip_code"`
	ComArea     string    `json:"community_area"`
	Coordinates []float64 `json:"coordinates"`
}

// Define global variables for covid package
var (
	logger   *log.Logger
	mu       sync.Mutex
	caLookup cazip.CALookupList
	zip_caLU []zipCaCovidLookup
)

func init() {
	// Open or create a log file
	currentTime := time.Now().Format("2006-01-02_15-04-05")
	logFilename := fmt.Sprintf("logfiles/covid_%s.log", currentTime)
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
func generator(done <-chan interface{}, covidList []covidCases) <-chan covidCases {
	covidStream := make(chan covidCases)
	go func() {
		defer close(covidStream)
		for _, cases := range covidList {
			select {
			case <-done:
				return
			case covidStream <- cases:
			}
		}
	}()
	return covidStream
}

func readLookup() {
	lookupfile, err := os.Open("covid/zip_ca_lookup.jl")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer lookupfile.Close()

	scanner := bufio.NewScanner(lookupfile)
	// Read each line from the file
	for scanner.Scan() {
		line := scanner.Bytes()

		// Unmarshal the JSON data into a struct
		var lu zipCaCovidLookup
		err := json.Unmarshal(line, &lu)
		if err != nil {
			fmt.Println("Error unmarshaling JSON:", err)
			continue
		}

		// Append the struct to the slice
		zip_caLU = append(zip_caLU, lu)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
}

func writeLookup() {
	lookupfile, err := os.Create("covid/zip_ca_lookup.jl")
	if err != nil {
		writeToLog("Error creating file: %s", err)
		fmt.Println("Error creating Covid lookup file:", err)
		return
	}

	defer lookupfile.Close()

	encoder := json.NewEncoder(lookupfile)

	for _, lu := range zip_caLU {
		err := encoder.Encode(lu)
		if err != nil {
			fmt.Println("error encoding JSON:", err)
			return
		}

	}
}

func BuildCovidTable() error {
	//Load lookup tables for use when cleaning
	caLookup = cazip.GetCaLookupList()
	readLookup()

	recordLimit := 5000

	covidURLs := []string{"https://data.cityofchicago.org/resource/yhhz-zm2v.json"}

	fmt.Println("Starting Build Covid")
	writeToLog("Starting Build Covid")

	// Make sure that the covid table is created
	err := createCovidTable()
	if err != nil {
		return fmt.Errorf("Error creating Covid19 table: %v", err)
	}

	queryURLs := common.BuildUrls(covidURLs, recordLimit)

	done := make(chan interface{})
	defer close(done)

	const maxWorkers = 10 // Limiting simultaneous API requests
	semaphore := make(chan struct{}, maxWorkers)

	var covidCasesList []covidCases

	for _, url := range queryURLs {
		semaphore <- struct{}{}
		go func(url string) {
			defer func() { <-semaphore }()
			for response := range getCovid(done, url) {
				if response.Error != nil {
					fmt.Println("Error:", response.Error)
					writeToLog("Error: %s", response.Error)
				} else {
					var cases []covidCases
					b, _ := io.ReadAll(response.Response.Body)
					json.Unmarshal(b, &cases)
					for i := range cases {
						cases[i].URL = url
					}
					covidCasesList = append(covidCasesList, cases...)
					writeToLog("Response: %s", response.Response.Status)
				}
			}
		}(url)
	}

	// Ensure all goroutines finish before exiting BuildTaxiTable
	for i := 0; i < maxWorkers; i++ {
		semaphore <- struct{}{}
	}

	fmt.Println("Number of covid cases records: ", len(covidCasesList))
	writeToLog("Completed API pull.")
	writeToLog("Number of covid cases records: %d", len(covidCasesList))

	fmt.Println("Starting pipeline to clean Covid19 data and write to PostgreSQL.")
	writeToLog("Starting pipeline to clean data and write to PostgreSQL.")
	covidStream := generator(done, covidCasesList)
	errCount := 0
	countWritten := 0
	countcovid := 0
	for cc := range cleanCovid(done, covidStream) {
		countcovid++
		written, err := addCovidRecord(cc)
		if err != nil {
			fmt.Println(err)
			writeToLog("Error writing %s?row_id=%s record to database.", cc.API, cc.RowID)
			errCount++
		}
		if written {
			countWritten++
		}
		if errCount > 10 {
			writeToLog("Too many errors writing to database. Stopping.")
			return fmt.Errorf("Error- too many errors writing to Covid database. Stopping.")
		}
	}

	// update lookup file with any new data
	writeLookup()

	fmt.Printf("Number of valid covid cases (not missing required values): %v\n", countcovid)
	fmt.Println("Finished Build Covid: ", countWritten)
	writeToLog("Finished Build Covid: %v", countWritten)

	return nil

}
