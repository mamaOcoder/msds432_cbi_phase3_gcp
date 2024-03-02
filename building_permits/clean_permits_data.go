package building_permits

import (
	"fmt"
	"p3-docker/cazip"
	"p3-docker/common"
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

/*
func CleanPermitTest() {
	caLookup = cazip.GetCaLookupList()
	zipLookup = cazip.GetZipLookupList()

	teststr := `{"id":"3100486","permit_":"100858762","permit_type":"PERMIT - NEW CONSTRUCTION","review_type":"STANDARD PLAN REVIEW","application_start_date":"2020-03-10T00:00:00.000","issue_date":"2020-08-24T00:00:00.000","processing_time":"167","street_number":"1759","street_direction":"S","street_name":"ELMHURST","suffix":"RD","work_description":"NEW CONSTRUCTION FOR A 2 STORY SOLID WASTE TRANSFER STATION AS PER PLANS. M3-2","building_fee_paid":"13960.44","zoning_fee_paid":"75","other_fee_paid":"0","subtotal_paid":"14035.44","building_fee_unpaid":"0","zoning_fee_unpaid":"0","other_fee_unpaid":"0","subtotal_unpaid":"0","building_fee_waived":"0","zoning_fee_waived":"0","other_fee_waived":"0","subtotal_waived":"0","total_fee":"14035.44","contact_1_type":"OWNER OCCUPIED","contact_1_name":"GROOT INDUSTRIES, INC.","contact_1_city":"ELK GROVE VILLAGE","contact_1_state":"IL","contact_1_zipcode":"60007","contact_2_type":"ARCHITECT","contact_2_name":"HARRIS KELLY P","contact_2_city":"PALATINE","contact_2_state":"IL","contact_2_zipcode":"60067","contact_3_type":"CONTRACTOR-ELECTRICAL","contact_3_name":"CONNELLY ELECTRIC CO.","contact_3_city":"ADDISON","contact_3_state":"IL","contact_3_zipcode":"60101","contact_4_type":"EXPEDITOR","contact_4_name":"BURNHAM NATIONWIDE, INC.","contact_4_city":"CHICAGO","contact_4_state":"IL","contact_4_zipcode":"60602-","contact_5_type":"CONTRACTOR-GENERAL CONTRACTOR","contact_5_name":"MORGAN HARBOUR CONSTRUCTION, LLC","contact_5_city":"WILLOWBROOK","contact_5_state":"IL","contact_5_zipcode":"60517-","contact_6_type":"MASONRY CONTRACTOR","contact_6_name":"II IN ONE CONTRACTORS, INC.","contact_6_city":"CHICAGO","contact_6_state":"IL","contact_6_zipcode":"60615","contact_7_type":"MASONRY CONTRACTOR","contact_7_name":"ROSEMONT MASONRY CORP.","contact_7_city":"ROSEMONT","contact_7_state":"IL","contact_7_zipcode":"60018","contact_8_type":"CONTRACTOR-PLUMBER/PLUMBING","contact_8_name":"MVP PLUMBING CORP","contact_8_city":"MONTGOMERY","contact_8_state":"IL","contact_8_zipcode":"60538-","contact_9_type":"CONTRACTOR-REFRIGERATION","contact_9_name":"SUN MECHANICAL SYSTEMS, INC","contact_9_city":"ST. CHARLES","contact_9_state":"IL","contact_9_zipcode":"60174-","contact_10_type":"CONTRACTOR-VENTILATION","contact_10_name":"SUN MECHANICAL SYSTEMS, INC","contact_10_city":"ST. CHARLES","contact_10_state":"IL","contact_10_zipcode":"60174-","reported_cost":"14612872","pin1":"08-36-300-130-0000","community_area":"76","census_tract":"980000","ward":"41","xcoordinate":"1091108.5636284188","ycoordinate":"1942715.3392330222","latitude":"41.999670854"}`

	var permit common.BuildingPermit
	err := json.Unmarshal([]byte(teststr), &permit)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	// check for messy/dirty/missing data values
	// Any record that has messy/dirty/missing data we don't enter in the data lake/table
	// Some rules:
	// Not null: PermitNum, PermitType, AppDate
	// AppDate and IssueDate need to be in format "0000-00-00T00:00:00.000"

	apiEndpoint, _ := common.ExtractAPI(permit.URL)
	permit.API = apiEndpoint

	if permit.ID == "" {
		writeToLog("Skipping record. Missing id.")
		return
	}
	if permit.PermitNum == "" {
		writeToLog("Skipping record %s?id=%s. Missing permit_number.", permit.API, permit.ID)
		return
	}
	if permit.PermitType == "" {
		writeToLog("Skipping record %s?id=%s. Missing permit_type.", permit.API, permit.ID)
		return
	}

	if common.CheckTimeFormat(permit.AppDate) == false {
		writeToLog("Skipping record %s?id=%s. Malformed application_start_date.", permit.API, permit.ID)
		return
	}

	if common.CheckTimeFormat(permit.IssueDate) == false {
		writeToLog("Skipping record %s?id=%s. Malformed issue_date.", permit.API, permit.ID)
		return
	}

	// Make sure we have location information
	// Best way to get zip code is using geos rather than community area
	// At this time, I am not using xcoord and ycoord because that requires additional calculations based on a specific projection
	caOnly := false
	if len(permit.Location.Coordinates) == 0 {
		if permit.Latitude == "" || permit.Longitude == "" {
			if permit.ComArea == "" {
				writeToLog("Skipping record %s?id=%s. Missing all location information.", permit.API, permit.ID)
				return
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

	fmt.Println(permit.ComArea)
	fmt.Println(permit)
}*/

func cleanPermit(done <-chan interface{}, permitStream <-chan common.BuildingPermit) <-chan common.BuildingPermit {
	cleanStream := make(chan common.BuildingPermit)
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
				return
			}
			if permit.PermitNum == "" {
				writeToLog("Skipping record %s?id=%s. Missing permit_number.", permit.API, permit.ID)
				return
			}
			if permit.PermitType == "" {
				writeToLog("Skipping record %s?id=%s. Missing permit_type.", permit.API, permit.ID)
				return
			}

			if common.CheckTimeFormat(permit.AppDate) == false {
				writeToLog("Skipping record %s?id=%s. Malformed application_start_date.", permit.API, permit.ID)
				return
			}

			if common.CheckTimeFormat(permit.IssueDate) == false {
				writeToLog("Skipping record %s?id=%s. Malformed issue_date.", permit.API, permit.ID)
				return
			}

			// Make sure we have location information
			// Best way to get zip code is using geos rather than community area
			// At this time, I am not using xcoord and ycoord because that requires additional calculations based on a specific projection
			caOnly := false
			if len(permit.Location.Coordinates) == 0 {
				if permit.Latitude == "" || permit.Longitude == "" {
					if permit.ComArea == "" {
						writeToLog("Skipping record %s?id=%s. Missing all location information.", permit.API, permit.ID)
						return
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
