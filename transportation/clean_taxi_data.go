package transportation

import (
	"fmt"
	"p3-gcp/common"
	"strconv"

	"github.com/paulmach/orb"
)

func cleanTaxi(done <-chan interface{}, taxiStream <-chan taxiTrip) <-chan taxiTrip {
	cleanStream := make(chan taxiTrip)
	go func() {
		defer close(cleanStream)
		for trip := range taxiStream {
			// check for messy/dirty/missing data values
			// Any record that has messy/dirty/missing data we don't enter in the data lake/table
			// Some rules:
			// Not null: TripID, TripStartTime, TripEndTime, PickupCA, DropoffCA, PickupLocation, DropoffLocation
			// If PickupLocation or DropoffLocation is empty, check lat/long to fill
			// TripStartTime and TripEndTime need to be in format "0000-00-00T00:00:00.000"

			apiEndpoint, _ := common.ExtractAPI(trip.URL)
			trip.API = apiEndpoint

			if trip.TripID == "" {
				writeToLog("Skipping trip. Missing trip_id.")
				continue
			}

			if common.CheckTimeFormat(trip.TripStartTime) == false {
				writeToLog("Skipping trip %s?trip_id=%s. Malformed trip_start_timestamp.", trip.API, trip.TripID)
				continue
			}

			if common.CheckTimeFormat(trip.TripEndTime) == false {
				writeToLog("Skipping trip %s?trip_id=%s. Malformed trip_end_timestamp.", trip.API, trip.TripID)
				continue
			}

			if len(trip.PickupLocation.Coordinates) == 0 {
				if trip.PickupLatitude == "" && trip.PickupLongitude == "" {
					writeToLog("Skipping trip %s?trip_id=%s. Missing Pickup Coordinates.", trip.API, trip.TripID)
					continue
				}
				// Convert strings to float64
				longitude, _ := strconv.ParseFloat(trip.PickupLongitude, 64)

				latitude, _ := strconv.ParseFloat(trip.PickupLatitude, 64)

				trip.PickupLocation.Type = "Point"
				trip.PickupLocation.Coordinates = []float64{longitude, latitude}

			}

			if len(trip.DropoffLocation.Coordinates) == 0 {
				if trip.DropoffLatitude == "" && trip.DropoffLongitude == "" {
					writeToLog("Skipping trip %s?trip_id=%s. Missing Dropoff Coordinates.", trip.API, trip.TripID)
					continue
				}
				// Convert strings to float64
				longitude, _ := strconv.ParseFloat(trip.DropoffLongitude, 64)

				latitude, _ := strconv.ParseFloat(trip.DropoffLatitude, 64)

				trip.DropoffLocation.Type = "Point"
				trip.DropoffLocation.Coordinates = []float64{longitude, latitude}

			}

			// If PickupCA is empty, try using coordinates to fill
			if trip.PickupCA == "" {
				gotpca := false
				for _, ca := range caLookup {
					if ca.Geometry.Coordinates.Bound().Contains(orb.Point(trip.PickupLocation.Coordinates)) {
						trip.PickupCA = ca.ComArea
						gotpca = true
						break
					}

				}
				if !gotpca {
					//add code to perform Google Geocoder reverse lookup
					fmt.Println("Could not find CA in lookup for", trip.PickupLocation.Coordinates)
				}
			}

			// If DropoffCA is empty, try using coordinates to fill
			if trip.DropoffCA == "" {
				gotdca := false
				for _, ca := range caLookup {
					if ca.Geometry.Coordinates.Bound().Contains(orb.Point(trip.DropoffLocation.Coordinates)) {
						trip.DropoffCA = ca.ComArea
						gotdca = true
						break
					}
				}
				if !gotdca {
					//add code to perform Google Geocoder reverse lookup
					fmt.Println("Could not find CA in lookup for", trip.DropoffLocation.Coordinates)
				}
			}

			// Get zip code
			gotpzip := false
			for _, zip := range zipLookup {
				if zip.Geometry.Coordinates.Bound().Contains(orb.Point(trip.PickupLocation.Coordinates)) {
					trip.PickupZipCode = zip.ZipCode
					gotpzip = true
					break
				}
			}
			if !gotpzip {
				//add code to perform Google Geocoder reverse lookup
				fmt.Println("Could not find Zip Code in lookup for", trip.PickupLocation.Coordinates)
			}

			gotdzip := false
			for _, zip := range zipLookup {
				if zip.Geometry.Coordinates.Bound().Contains(orb.Point(trip.DropoffLocation.Coordinates)) {
					trip.DropoffZipCode = zip.ZipCode
					gotdzip = true
					break
				}
			}
			if !gotdzip {
				//add code to perform Google Geocoder reverse lookup
				fmt.Println("Could not find Zip Code in lookup for", trip.PickupLocation.Coordinates)
			}

			select {
			case <-done:
				return
			case cleanStream <- trip:
			}
		}
	}()
	return cleanStream

}
