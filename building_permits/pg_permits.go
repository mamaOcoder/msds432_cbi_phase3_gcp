package building_permits

import (
	"fmt"
	"p3-gcp/common"
	"strconv"
)

// PostgreSQL code for Unemployment Data
func createPermitTable() error {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits" (
		"id" VARCHAR(255) UNIQUE,
		"permit_number" VARCHAR(255),
		"permit_type" VARCHAR(255),
		"application_start_date" TIMESTAMP WITH TIME ZONE,
		"issue_date" TIMESTAMP WITH TIME ZONE,
		"work_description" TEXT,
		"total_fee" DOUBLE PRECISION,
		"reported_cost" DOUBLE PRECISION,
		"community_area" VARCHAR(255),
		"xcoordinate" DOUBLE PRECISION,
		"ycoordinate" DOUBLE PRECISION,
		"latitude" DOUBLE PRECISION,
		"longitude" DOUBLE PRECISION,
		"zip_code" VARCHAR(255),
		"api_endpoint" VARCHAR(255),
		PRIMARY KEY ("id") 
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		fmt.Println("Couldn't create table building_permits")
		writeToLog("Couldn't create table building_permits")
		return _err
	}

	writeToLog("Table building_permits created successfully")
	return nil
}

func permitExists(permit string) bool {
	exists := false
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return exists
	}
	defer db.Close()

	sql := fmt.Sprintf(`SELECT "id" FROM "building_permits" where "id" = '%s'`, permit)
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

func addPermitRecord(permit buildingPermit) (bool, error) {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return false, err
	}
	defer db.Close()

	if permitExists(permit.ID) {
		writeToLog("Skipping record %s. Already exists in database.", permit.ID)
		return false, nil
	}

	sql := `INSERT INTO building_permits ("id",
										"permit_number",
										"permit_type",
										"application_start_date",
										"issue_date",
										"work_description",
										"total_fee",
										"reported_cost",
										"community_area",
										"xcoordinate",
										"ycoordinate",
										"latitude",
										"longitude",
										"zip_code",
										"api_endpoint") 
			values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`

	// convert string to floats
	tf, _ := strconv.ParseFloat(permit.TotalFee, 64)
	rc, _ := strconv.ParseFloat(permit.ReportedCost, 64)
	xc, _ := strconv.ParseFloat(permit.XCoord, 64)
	yc, _ := strconv.ParseFloat(permit.YCoord, 64)
	lat, _ := strconv.ParseFloat(permit.Latitude, 64)
	lon, _ := strconv.ParseFloat(permit.Longitude, 64)

	// Convert the array of float64 coordinates into a valid geography type
	//loc := fmt.Sprintf("SRID=4326;POINT(%f %f)", permit.Location.Coordinates[0], permit.Location.Coordinates[1])

	_, err = db.Exec(sql,
		permit.ID,
		permit.PermitNum,
		permit.PermitType,
		permit.AppDate,
		permit.IssueDate,
		permit.WorkDesc,
		tf,
		rc,
		permit.ComArea,
		xc,
		yc,
		lat,
		lon,
		permit.ZipCode,
		permit.API)

	if err != nil {
		writeToLog("Couldn't write %s record to building_permit table", permit.ID)
		return false, err
	}

	return true, nil
}
