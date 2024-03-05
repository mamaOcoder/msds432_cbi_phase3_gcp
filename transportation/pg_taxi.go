package transportation

import (
	"fmt"
	"p3-gcp/common"
)

// PostgreSQL code for transportation data
func createTaxiTable() error {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	create_table := `CREATE TABLE IF NOT EXISTS "taxi_trips" (
		"trip_id" VARCHAR(255) UNIQUE, 
		"taxi_id" VARCHAR(255),
		"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
		"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
		"company" VARCHAR(255),
		"pickup_community_area" VARCHAR(255),
		"dropoff_community_area" VARCHAR(255),
		"pickup_centroid_latitude" DOUBLE PRECISION, 
		"pickup_centroid_longitude" DOUBLE PRECISION, 
		"dropoff_centroid_latitude" DOUBLE PRECISION, 
		"dropoff_centroid_longitude" DOUBLE PRECISION, 
		"pickup_zip_code" VARCHAR(255), 
		"dropoff_zip_code" VARCHAR(255), 
		"api_endpoint" VARCHAR(255),
		PRIMARY KEY ("trip_id") 
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		fmt.Println("Couldn't create table taxi_trips")
		writeToLog("Couldn't create table taxi_trips")
		return _err
	}

	writeToLog("Table taxi_trips created successfully")
	return nil
}
func tripExists(trip string) bool {
	exists := false
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return exists
	}
	defer db.Close()

	sql := fmt.Sprintf(`SELECT "trip_id" FROM "taxi_trips" where "trip_id" = '%s'`, trip)
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Println("Query error:", err)
		return exists
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			fmt.Println("Scan", err)
			return exists
		}
		exists = true
	}

	return exists
}

func addTaxiTrip(taxi taxiTrip) (bool, error) {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return false, err
	}
	defer db.Close()

	if tripExists(taxi.TripID) {
		writeToLog("Skipping trip %s. Already exists in database.", taxi.TripID)
		return false, nil
	}

	sql := `INSERT INTO taxi_trips ("trip_id", 
									"taxi_id",
									"trip_start_timestamp", 
									"trip_end_timestamp", 
									"company",
									"pickup_community_area",
									"dropoff_community_area",
									"pickup_centroid_latitude", 
									"pickup_centroid_longitude", 
									"dropoff_centroid_latitude", 
									"dropoff_centroid_longitude", 
									"pickup_zip_code", 
									"dropoff_zip_code", 
									"api_endpoint") 
			values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

	// Convert the array of float64 coordinates into a valid geography type
	//pickupLoc := fmt.Sprintf("SRID=4326;POINT(%f %f)", taxi.PickupLocation.Coordinates[0], taxi.PickupLocation.Coordinates[1])
	//dropoffLoc := fmt.Sprintf("SRID=4326;POINT(%f %f)", taxi.DropoffLocation.Coordinates[0], taxi.DropoffLocation.Coordinates[1])

	_, err = db.Exec(sql,
		taxi.TripID,
		taxi.TaxiID,
		taxi.TripStartTime,
		taxi.TripEndTime,
		taxi.Company,
		taxi.PickupCA,
		taxi.DropoffCA,
		taxi.PickupLatitude,
		taxi.PickupLongitude,
		taxi.DropoffLatitude,
		taxi.DropoffLongitude,
		taxi.PickupZipCode,
		taxi.DropoffZipCode,
		taxi.API)

	if err != nil {
		writeToLog("Couldn't write %s trip to taxi table", taxi.TripID)
		return false, err
	}

	return true, nil
}
