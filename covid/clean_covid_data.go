package covid

import (
	"fmt"
	"p3-gcp/common"

	"github.com/paulmach/orb"
)

func cleanCovid(done <-chan interface{}, covidStream <-chan covidCases) <-chan covidCases {
	cleanStream := make(chan covidCases)
	go func() {
		defer close(cleanStream)
		for record := range covidStream {
			// check for messy/dirty/missing data values
			// Any record that has messy/dirty/missing data we don't enter in the data lake/table
			// Some rules:
			// Not null: RowID, ZipCode, WeekNum, WeekStart, WeekEnd, CasesWeekly, Location
			// WeekStart and WeekEnd need to be in format "0000-00-00T00:00:00.000"
			apiEndpoint, _ := common.ExtractAPI(record.URL)
			record.API = apiEndpoint

			if record.RowID == "" {
				fmt.Printf("Skipping record. Missing row_id.\n")
				writeToLog("Skipping record. Missing row_id.")
				return
			}
			if record.ZipCode == "" {
				fmt.Printf("Skipping record %s?row_id=%s. Missing zip_code.\n", record.API, record.RowID)
				writeToLog("Skipping record %s?row_id=%s. Missing zip_code.", record.API, record.RowID)
				return
			}
			if record.WeekNum == "" {
				fmt.Printf("Skipping record %s?row_id=%s. Missing week_number.\n", record.API, record.RowID)
				writeToLog("Skipping record %s?row_id=%s. Missing week_number.", record.API, record.RowID)
				return
			}

			//City of Chicago leaves this blank until count reaches 5. We will impute blank value to 0.
			if record.CasesWeekly == "" {
				record.CasesWeekly = "0"
			}

			if common.CheckTimeFormat(record.WeekStart) == false {
				fmt.Printf("Skipping record %s?row_id=%s. Malformed week_start.\n", record.API, record.RowID)
				writeToLog("Skipping record %s?row_id=%s. Malformed week_start.", record.API, record.RowID)
				return
			}

			if common.CheckTimeFormat(record.WeekEnd) == false {
				fmt.Printf("Skipping record %s?row_id=%s. Malformed week_end.\n", record.API, record.RowID)
				writeToLog("Skipping record %s?row_id=%s. Malformed week_end.", record.API, record.RowID)
				return
			}

			if len(record.Location.Coordinates) == 0 {
				fmt.Printf("Skipping record %s?row_id=%s. Missing zip_code_location coordinates.\n", record.API, record.RowID)
				writeToLog("Skipping record %s?row_id=%s. Missing zip_code_location coordinates.", record.API, record.RowID)
				return
			}

			//Get community area
			gotca := false
			for _, ca := range caLookup {
				if ca.Geometry.Coordinates.Bound().Contains(orb.Point(record.Location.Coordinates)) {
					record.ComArea = ca.ComArea
					gotca = true
					break
				}
			}
			if !gotca {
				// First check if we have already performed reverse lookup for this zip code
				inlu := false
				for _, lu := range zip_caLU {
					if lu.ZipCode == record.ZipCode {
						record.ComArea = lu.ComArea
						inlu = true
						break
					}
				}
				if !inlu {
					//Perform Google Geocoder reverse lookup
					fmt.Printf("Could not find Community Area in lookup for %v, %s. Performing geocoder reverse lookup.\n", record.Location.Coordinates, record.ZipCode)
					writeToLog("Could not find Community Area in lookup for %v, %s. Performing geocoder reverse lookup.", record.Location.Coordinates, record.ZipCode)
					ca, err := common.CaFromGeo(record.Location.Coordinates)
					if err != nil {
						fmt.Println(err)
						return
					}
					record.ComArea = ca
					addLookup := zipCaCovidLookup{
						ZipCode:     record.ZipCode,
						ComArea:     ca,
						Coordinates: record.Location.Coordinates,
					}
					zip_caLU = append(zip_caLU, addLookup)
				}

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
