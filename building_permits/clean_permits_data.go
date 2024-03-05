package building_permits

import (
	"fmt"
	"p3-gcp/cazip"
	"p3-gcp/common"
	"strconv"

	"github.com/paulmach/orb"
)

func caToZip(comarea string) string {
	caLookup = cazip.GetCaLookupList()
	zipLookup = cazip.GetZipLookupList()
	zipstr := ""
	for _, ca := range caLookup {
		if ca.ComArea != comarea {
			continue
		}
		// *TODO* Consider calculating area of the intersection to better determine which zip codes represent the community area
		// It is possible that multiple zip codes could be within a community area
		// We will list them in a string
		for _, zip := range zipLookup {
			if ca.Geometry.Coordinates.Bound().Intersects(zip.Geometry.Coordinates.Bound()) {
				if zipstr == "" {
					zipstr += zip.ZipCode
				} else {
					zipstr += fmt.Sprintf(", %s", zip.ZipCode)
				}
			}
		}
	}
	return zipstr
}

func cleanPermit(done <-chan interface{}, permitStream <-chan buildingPermit) <-chan buildingPermit {
	cleanStream := make(chan buildingPermit)
	go func() {
		defer close(cleanStream)
		for permit := range permitStream {
			// check for messy/dirty/missing data values
			// Any record that has messy/dirty/missing data we don't enter in the data lake/table
			// Some rules:
			// Not null: PermitNum, PermitType, AppDate
			// AppDate and IssueDate need to be in format "0000-00-00T00:00:00.000"

			apiEndpoint, _ := common.ExtractAPI(permit.URL)
			permit.API = apiEndpoint

			if permit.ID == "" {
				writeToLog("Skipping record. Missing id.")
				continue
			}

			if permit.PermitNum == "" {
				writeToLog("Skipping record %s?id=%s. Missing permit_number.", permit.API, permit.ID)
				continue
			}
			if permit.PermitType == "" {
				writeToLog("Skipping record %s?id=%s. Missing permit_type.", permit.API, permit.ID)
				continue
			}

			if common.CheckTimeFormat(permit.AppDate) == false {
				writeToLog("Skipping record %s?id=%s. Malformed application_start_date.", permit.API, permit.ID)
				continue
			}

			if common.CheckTimeFormat(permit.IssueDate) == false {
				writeToLog("Skipping record %s?id=%s. Malformed issue_date.", permit.API, permit.ID)
				continue
			}

			// Make sure we have location information
			// Best way to get zip code is using geos rather than community area
			// At this time, I am not using xcoord and ycoord because that requires additional calculations based on a specific projection
			caOnly := false
			if len(permit.Location.Coordinates) == 0 {
				if permit.Latitude == "" || permit.Longitude == "" {
					if permit.ComArea == "" {
						writeToLog("Skipping record %s?id=%s. Missing all location information.", permit.API, permit.ID)
						continue
					}
					// we will have to use community area to determine zip code
					caOnly = true
				}
				// Convert strings to float64
				longitude, _ := strconv.ParseFloat(permit.Longitude, 64)

				latitude, _ := strconv.ParseFloat(permit.Latitude, 64)

				permit.Location.Type = "Point"
				permit.Location.Coordinates = []float64{longitude, latitude}

			}

			// Get community area if we don't have it
			if permit.ComArea == "" {
				gotca := false
				for _, ca := range caLookup {
					if ca.Geometry.Coordinates.Bound().Contains(orb.Point(permit.Location.Coordinates)) {
						permit.ComArea = ca.ComArea
						gotca = true
						break
					}
				}
				if !gotca {
					//add code to perform Google Geocoder reverse lookup
					fmt.Println("Could not find CA in lookup for", permit.Location.Coordinates)
				}
			}

			// Get zip code
			// Use community area to determine zipcode
			if caOnly {
				permit.ZipCode = caToZip(permit.ComArea)
			} else {
				gotzip := false
				for _, zip := range zipLookup {
					if zip.Geometry.Coordinates.Bound().Contains(orb.Point(permit.Location.Coordinates)) {
						permit.ZipCode = zip.ZipCode
						gotzip = true
						break
					}
				}
				if !gotzip {
					//add code to perform Google Geocoder reverse lookup
					fmt.Println("Could not find Zip Code in lookup for", permit.Location.Coordinates)
				}

			}

			select {
			case <-done:
				return
			case cleanStream <- permit:
			}
		}

	}()
	return cleanStream
}
