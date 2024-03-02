package main

import (
	"fmt"
	"p3-docker/building_permits"
	"p3-docker/cazip"
	"p3-docker/ccvi"
	"p3-docker/covid"
	"p3-docker/postgres"
	"p3-docker/transportation"
	"p3-docker/unemployment"
	"time"
)

func main() {

	start := time.Now()

	// Lookup table needs to be built before others because they will use it
	cazip.BuildLookups()

	err := postgres.CreateDatabase()
	if err != nil {
		fmt.Println("Failed to create database:", err)
		return
	}

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

	fmt.Println("Total time taken:", time.Since(start))
}
