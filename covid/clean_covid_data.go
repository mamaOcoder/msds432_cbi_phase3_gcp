package covid

import (
	"fmt"
	"p3-docker/common"

	"github.com/paulmach/orb"
)

/*
	type zipCaCovidLookup struct {
		ZipCode     string
		ComArea     string
		Coordinates []float64
	}

	func CleanCovidTest() {
		caLookup = cazip.GetCaLookupList()
		var zip_caLU []zipCaCovidLookup
		teststr := `{"zip_code":"60707","week_number":"19","week_start":"2020-05-03T00:00:00.000","week_end":"2020-05-09T00:00:00.000","cases_weekly":"61","cases_cumulative":"262","case_rate_weekly":"141.8","case_rate_cumulative":"609","tests_weekly":"328","tests_cumulative":"1046","test_rate_weekly":"762.5","test_rate_cumulative":"2431.5","percent_tested_positive_weekly":"0.213","percent_tested_positive_cumulative":"0.24","deaths_weekly":"5","deaths_cumulative":"9","death_rate_weekly":"11.6","death_rate_cumulative":"20.9","population":"43019","row_id":"60707-2020-19","zip_code_location":{"type":"Point","coordinates":[-87.808283,41.921777]}}`

		var covidRecord common.CovidCases
		err := json.Unmarshal([]byte(teststr), &covidRecord)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		// check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter in the data lake/table
		// Some rules:
		// Not null: RowID, ZipCode, WeekNum, WeekStart, WeekEnd, CasesWeekly, Location
		// WeekStart and WeekEnd need to be in format "0000-00-00T00:00:00.000"

		apiEndpoint, _ := common.ExtractAPI(covidRecord.URL)
		covidRecord.API = apiEndpoint

		if covidRecord.RowID == "" {
			writeToLog("Skipping record. Missing row_id.")
			return
		}
		if covidRecord.ZipCode == "" {
			writeToLog("Skipping record %s?row_id=%s. Missing zip_code.", covidRecord.API, covidRecord.RowID)
			return
		}
		if covidRecord.WeekNum == "" {
			writeToLog("Skipping record %s?row_id=%s. Missing week_number.", covidRecord.API, covidRecord.RowID)
			return
		}

		//City of Chicago leaves this blank until count reaches 5. We will impute blank value to 0.
		if covidRecord.CasesWeekly == "" {
			covidRecord.CasesWeekly = "0"
		}

		if common.CheckTimeFormat(covidRecord.WeekStart) == false {
			writeToLog("Skipping record %s?row_id=%s. Malformed week_start.", covidRecord.API, covidRecord.RowID)
			return
		}

		if common.CheckTimeFormat(covidRecord.WeekEnd) == false {
			writeToLog("Skipping record %s?row_id=%s. Malformed week_end.", covidRecord.API, covidRecord.RowID)
			return
		}

		if len(covidRecord.Location.Coordinates) == 0 {
			writeToLog("Skipping record %s?row_id=%s. Missing zip_code_location coordinates.", covidRecord.API, covidRecord.RowID)
			return
		}

		//Get community area
		gotca := false
		for _, ca := range caLookup {
			if ca.Geometry.Coordinates.Bound().Contains(orb.Point(covidRecord.Location.Coordinates)) {
				covidRecord.ComArea = ca.ComArea
				gotca = true
				break
			}
		}
		if !gotca {
			// First check if we have already performed reverse lookup for this zip code
			inlu := false
			for _, lu := range zip_caLU {
				if lu.ZipCode == covidRecord.ZipCode {
					covidRecord.ComArea = lu.ComArea
					inlu = true
					break
				}
			}
			if !inlu {
				//Perform Google Geocoder reverse lookup
				fmt.Println("Could not find Community Area in lookup for", covidRecord.Location.Coordinates)
				ca, err := common.CaFromGeo(covidRecord.Location.Coordinates)
				if err != nil {
					fmt.Println(err)
					return
				}
				covidRecord.ComArea = ca
				addLookup := zipCaCovidLookup{
					ZipCode:     covidRecord.ZipCode,
					ComArea:     ca,
					Coordinates: covidRecord.Location.Coordinates,
				}
				zip_caLU = append(zip_caLU, addLookup)

			}

		}

		fmt.Println(covidRecord)
		fmt.Println(zip_caLU)
	}
*/
func cleanCovid(done <-chan interface{}, covidStream <-chan common.CovidCases) <-chan common.CovidCases {
	cleanStream := make(chan common.CovidCases)
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
				writeToLog("Skipping record. Missing row_id.")
				return
			}
			if record.ZipCode == "" {
				writeToLog("Skipping record %s?row_id=%s. Missing zip_code.", record.API, record.RowID)
				return
			}
			if record.WeekNum == "" {
				writeToLog("Skipping record %s?row_id=%s. Missing week_number.", record.API, record.RowID)
				return
			}

			//City of Chicago leaves this blank until count reaches 5. We will impute blank value to 0.
			if record.CasesWeekly == "" {
				record.CasesWeekly = "0"
			}

			if common.CheckTimeFormat(record.WeekStart) == false {
				writeToLog("Skipping record %s?row_id=%s. Malformed week_start.", record.API, record.RowID)
				return
			}

			if common.CheckTimeFormat(record.WeekEnd) == false {
				writeToLog("Skipping record %s?row_id=%s. Malformed week_end.", record.API, record.RowID)
				return
			}

			if len(record.Location.Coordinates) == 0 {
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
