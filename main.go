package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"p3-docker/building_permits"
	"p3-docker/cazip"
	"p3-docker/ccvi"
	"p3-docker/covid"
	"p3-docker/transportation"
	"p3-docker/unemployment"
	"time"
)

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("PROJECT_ID")
	if name == "" {
		name = "CBI-Project"
	}

	fmt.Fprintf(w, "CBI data collection microservices' goroutines have started for %s!\n", name)
}

func main() {

	for {

		// While using Cloud Run for instrumenting/prototyping/debugging use the server
		// to trace the state of you running data collection services
		// Navigate to Cloud Run services and find the URL of your service
		// An example of your services URL: https://go-microservice-23zzuv4hksp-uc.a.run.app
		// Use the browser and navigate to your service URL to to kick-start your service
		start := time.Now()
		log.Print("Starting CBI Microservices ...")
		// Lookup table needs to be built before others because they will use it
		cazip.BuildLookups()

		/*err := postgres.CreateDatabase()
		if err != nil {
			fmt.Println("Failed to create database:", err)
			return
		}*/

		// Channel to synchronize completion of tasks
		done := make(chan struct{})
		defer close(done)

		// Channel to handle errors
		errCh := make(chan error, 5) // Buffer size 5 to handle errors from 5 tasks

		// Concurrently execute tasks
		go func() {
			errCh <- transportation.BuildTaxiTable() // Build taxi table
		}()

		go func() {
			errCh <- covid.BuildCovidTable() // Build Covid table
		}()

		go func() {
			errCh <- ccvi.BuildCcviTable() // Build CCVI table
		}()

		go func() {
			errCh <- unemployment.BuildUnemploymentTable() // Build unemployment table
		}()

		go func() {
			errCh <- building_permits.BuildPermitsTable()
		}()

		// Wait for all tasks to complete
		for i := 0; i < 5; i++ { // Waiting for 5 tasks to complete
			if err := <-errCh; err != nil {
				fmt.Println("Error:", err)
			}
		}

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

		fmt.Println("Total time taken:", time.Since(start))

		time.Sleep(24 * time.Hour)
	}
}
