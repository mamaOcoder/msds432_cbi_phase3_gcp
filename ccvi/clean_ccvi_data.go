package ccvi

import (
	"fmt"
	"p3-gcp/common"
)

func cleanCCVI(done <-chan interface{}, ccviStream <-chan ccvi) <-chan ccvi {
	cleanStream := make(chan ccvi)
	go func() {
		defer close(cleanStream)
		for record := range ccviStream {
			// Check for messy/dirty/missing data values
			// Any record that has messy/dirty/missing data we don't enter in the data lake/table
			// Some rules:
			// Not null: GeographyType, CAorZip, CcviScore, CcviCategory, Location
			// Record has either the community area number or the zip code, not both, so we will need to parse these
			// We do not need to do any lookups because the data already returns the values for each zip code or community area
			apiEndpoint, _ := common.ExtractAPI(record.URL)
			record.API = apiEndpoint

			if record.GeographyType == "" {
				writeToLog("Skipping record. Missing geography_type.")
				continue
			}
			if record.CAorZip == "" {
				writeToLog("Skipping record. Missing community_area_or_zip.")
				continue
			}
			if record.CcviScore == "" {
				writeToLog("Skipping record. Missing ccvi_score.")
				continue
			}
			if record.CcviCategory == "" {
				writeToLog("Skipping record. Missing ccvi_category.")
				continue
			}

			if len(record.Location.Coordinates) == 0 {
				writeToLog("Skipping record. Missing location coordinates.")
				continue
			}

			// Split CA and zip code records for easier/consistent querying of our data lake
			if record.GeographyType == "CA" {
				record.ComArea = record.CAorZip
			}
			if record.GeographyType == "ZIP" {
				record.ZipCode = record.CAorZip
			}

			// Make an ID for the data using geo type and ca or zip
			record.ID = fmt.Sprintf("%s-%s", record.GeographyType, record.CAorZip)

			select {
			case <-done:
				return
			case cleanStream <- record:
			}
		}
	}()
	return cleanStream
}
