package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"p3-gcp/building_permits"
	"p3-gcp/cazip"
	"p3-gcp/common"
	"p3-gcp/covid"
	"p3-gcp/transportation"
	"time"

	"github.com/robfig/cron/v3"
)

func init() {

	// Create logfiles folder if it doesn't exist
	if _, err := os.Stat("logfiles"); os.IsNotExist(err) {
		err := os.Mkdir("logfiles", 0755)
		if err != nil {
			log.Fatal("Failed to create logfiles folder:", err)
		}
	}

	// Lookup table needs to be built before others because they will use it
	cazip.BuildLookups()

	err := common.CreateDatabase()
	if err != nil {
		fmt.Println("Failed to create database:", err)
		return
	}
}

func allTasks() {

	start := time.Now()
	// Channel to synchronize completion of tasks
	done := make(chan struct{})
	defer close(done)

	// Channel to handle errors
	errCh := make(chan error, 1) // Buffer size 5 to handle errors from 5 tasks

	// Concurrently execute tasks
	/*go func() {
		errCh <- transportation.BuildTaxiTable() // Build taxi table
	}()*/
	go func() {
		errCh <- covid.BuildCovidTable() // Build Covid table
	}()
	/*go func() {
		errCh <- ccvi.BuildCcviTable() // Build CCVI table
	}()
	go func() {
		errCh <- unemployment.BuildUnemploymentTable() // Build unemployment table
	}()
	go func() {
		errCh <- building_permits.BuildPermitsTable()
	}()*/

	// Wait for all tasks to complete
	for i := 0; i < 1; i++ { // Waiting for 5 tasks to complete
		if err := <-errCh; err != nil {
			fmt.Println("Error:", err)
		}
	}
	fmt.Println("Total time taken:", time.Since(start))

}

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("PROJECT_ID")
	if name == "" {
		name = "MSDS432-CBI-Phase3"
	}

	fmt.Fprintf(w, "CBI data collection microservices' goroutines have started for %s!\n", name)
}

func main() {

	// While using Cloud Run for instrumenting/prototyping/debugging use the server
	// to trace the state of you running data collection services
	// Navigate to Cloud Run services and find the URL of your service
	// An example of your services URL: https://go-microservice-23zzuv4hksp-uc.a.run.app
	// Use the browser and navigate to your service URL to to kick-start your service

	log.Print("Starting CBI Microservices ...")

	// Initial run of all tasks

	go allTasks()

	http.HandleFunc("/", handler)

	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("defaulting to port %s", port)
	}

	// Start HTTP server.
	log.Printf("listening on port %s", port)
	log.Print("Navigate to Cloud Run services and find the URL of your service")
	log.Print("Use the browser and navigate to your service URL to to check your service has started")

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}

	// Define cron jobs to look for updates from the City of Chicago database
	scheduler := cron.New()

	// Taxi databases get updated monthly
	_, err := scheduler.AddFunc("@monthly", func() {
		transportation.BuildTaxiTable()
	})
	if err != nil {
		fmt.Println("Error adding cron job for BuildTaxiTable:", err)
	}

	// Covid database gets updated weekly
	_, err = scheduler.AddFunc("@weekly", func() {
		covid.BuildCovidTable()
	})
	if err != nil {
		fmt.Println("Error adding cron job for BuildCovidTable:", err)
	}

	// Building permits database gets updated daily
	_, err = scheduler.AddFunc("@daily", func() {
		building_permits.BuildPermitsTable()
	})
	if err != nil {
		fmt.Println("Error adding cron job for BuildPermitsTable:", err)
	}

	// Start the cron scheduler
	scheduler.Start()

	// Keep the program running
	select {}

}
