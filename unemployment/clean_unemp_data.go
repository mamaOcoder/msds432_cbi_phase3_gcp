package unemployment

import (
	"fmt"
	"p3-gcp/common"
)

func cleanUnemployment(done <-chan interface{}, unempStream <-chan unemployment) <-chan unemployment {
	cleanStream := make(chan unemployment)
	go func() {
		defer close(cleanStream)
		for record := range unempStream {
			// Check for messy/dirty/missing data values
			// Any record that has messy/dirty/missing data we don't enter in the data lake/table
			// Some rules:
			// Not null: ComArea, BelowPL, Unemployment
			// There are no coordinated provided in this dataset, so we will need to use the ComArea to determine ZipCode

			apiEndpoint, _ := common.ExtractAPI(record.URL)
			record.API = apiEndpoint

			if record.ComArea == "" {
				writeToLog("Skipping record. Missing community_area.")
				continue
			}
			if record.BelowPL == "" {
				writeToLog("Skipping record. Missing below_poverty_level.")
				continue
			}
			if record.Unemployment == "" {
				writeToLog("Skipping record. Missing unemployment.")
				continue
			}

			// For better efficiency we could rewrite the lookup as a map
			// but since this is the only dataset that would need it in that format
			// and there is only 77 records, we will not worry about it.
			for _, ca := range caLookup {
				if ca.ComArea != record.ComArea {
					continue
				}
				// *TODO* Consider calculating area of the intersection to better determine which zip codes represent the community area
				// It is possible that multiple zip codes could be within a community area
				// We will list them in a string
				zipstr := ""
				for _, zip := range zipLookup {
					if ca.Geometry.Coordinates.Bound().Intersects(zip.Geometry.Coordinates.Bound()) {
						if zipstr == "" {
							zipstr += zip.ZipCode
						} else {
							zipstr += fmt.Sprintf(", %s", zip.ZipCode)
						}
					}
				}
				record.ZipCode = zipstr

			}

			select {
			case <-done:
				return
			case cleanStream <- record:
			}
		}
	}()
	return cleanStream
}
