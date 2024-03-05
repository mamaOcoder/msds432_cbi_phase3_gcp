package ccvi

import (
	"fmt"
	"p3-gcp/common"
)

// PostgreSQL code for CCVI Data
func createCcviTable() error {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	create_table := `CREATE TABLE IF NOT EXISTS "ccvi" (
		"id" VARCHAR(255) UNIQUE,
		"geography_type" VARCHAR(255), 
		"community_area" VARCHAR(255),
		"community_area_name" VARCHAR(255),
		"zip_code" VARCHAR(255),
		"ccvi_category" VARCHAR(255),
		"ccvi_score" DOUBLE PRECISION,
		"latitude" DOUBLE PRECISION,
		"longitude" DOUBLE PRECISION,
		"api_endpoint" VARCHAR(255),
		PRIMARY KEY ("id") 
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		fmt.Println("Couldn't create table ccvi")
		writeToLog("Couldn't create table ccvi")
		return _err
	}

	writeToLog("Table ccvi created successfully")
	return nil
}

func ccviExists(ccvi string) bool {
	exists := false
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return exists
	}
	defer db.Close()

	sql := fmt.Sprintf(`SELECT "id" FROM "ccvi" where "id" = '%s'`, ccvi)
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

func addCcviRecord(ccvi ccvi) (bool, error) {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return false, err
	}
	defer db.Close()

	if ccviExists(ccvi.ID) {
		writeToLog("Skipping record %s. Already exists in database.", ccvi.ID)
		return false, nil
	}

	sql := `INSERT INTO ccvi ("id", 
							"geography_type",
							"community_area",
							"community_area_name", 
							"zip_code", 
							"ccvi_category",
							"ccvi_score",
							"latitude",
							"longitude",
							"api_endpoint") 
			values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	// Convert the array of float64 coordinates into a valid geography type
	//loc := fmt.Sprintf("SRID=4326;POINT(%f %f)", ccvi.Location.Coordinates[0], ccvi.Location.Coordinates[1])
	lat := ccvi.Location.Coordinates[1]
	lon := ccvi.Location.Coordinates[0]

	_, err = db.Exec(sql,
		ccvi.ID,
		ccvi.GeographyType,
		ccvi.ComArea,
		ccvi.CAName,
		ccvi.ZipCode,
		ccvi.CcviCategory,
		ccvi.CcviScore,
		lat,
		lon,
		ccvi.API)

	if err != nil {
		writeToLog("Couldn't write %s record to ccvi table", ccvi.ID)
		return false, err
	}

	return true, nil
}
