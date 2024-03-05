package covid

import (
	"fmt"
	"p3-gcp/common"
)

// PostgreSQL code for Covid 19 Cases Data
func createCovidTable() error {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	create_table := `CREATE TABLE IF NOT EXISTS "covid19_cases" (
		"row_id" VARCHAR(255) UNIQUE, 
		"zip_code" VARCHAR(255),
		"week_number" VARCHAR(255),
		"week_start" TIMESTAMP WITH TIME ZONE, 
		"week_end" TIMESTAMP WITH TIME ZONE, 
		"cases_weekly" INTEGER,
		"latitude" DOUBLE PRECISION,
		"longitude" DOUBLE PRECISION,
		"community_area" VARCHAR(255),
		"api_endpoint" VARCHAR(255),
		PRIMARY KEY ("row_id") 
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		fmt.Println("Couldn't create table covid19_cases")
		writeToLog("Couldn't create table covid19_cases")
		return _err
	}

	writeToLog("Table covid19_cases created successfully")
	return nil
}

func covidExists(covid string) bool {
	exists := false
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return exists
	}
	defer db.Close()

	sql := fmt.Sprintf(`SELECT "row_id" FROM "covid19_cases" where "row_id" = '%s'`, covid)
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

func addCovidRecord(covid covidCases) (bool, error) {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return false, err
	}
	defer db.Close()

	if covidExists(covid.RowID) {
		writeToLog("Skipping record %s. Already exists in database.", covid.RowID)
		return false, nil
	}

	sql := `INSERT INTO covid19_cases ("row_id", 
										"zip_code",
										"week_number",
										"week_start", 
										"week_end", 
										"cases_weekly",
										"latitude",
										"longitude",
										"community_area",
										"api_endpoint") 
			values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	// Convert the array of float64 coordinates into a valid geography type
	//loc := fmt.Sprintf("SRID=4326;POINT(%f %f)", covid.Location.Coordinates[0], covid.Location.Coordinates[1])
	lat := covid.Location.Coordinates[1]
	lon := covid.Location.Coordinates[0]

	_, err = db.Exec(sql,
		covid.RowID,
		covid.ZipCode,
		covid.WeekNum,
		covid.WeekStart,
		covid.WeekEnd,
		covid.CasesWeekly,
		lat,
		lon,
		covid.ComArea,
		covid.API)

	if err != nil {
		writeToLog("Couldn't write %s record to covid19 table", covid.RowID)
		return false, err
	}

	return true, nil
}
