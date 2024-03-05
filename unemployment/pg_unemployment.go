package unemployment

import (
	"fmt"
	"p3-gcp/common"
	"strconv"
)

// PostgreSQL code for Unemployment Data
func createUnemploymentTable() error {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	create_table := `CREATE TABLE IF NOT EXISTS "unemployment" (
		"id"   SERIAL ,
		"community_area" VARCHAR(255),
		"community_area_name" VARCHAR(255),
		"below_poverty_level" DOUBLE PRECISION,
		"crowded_housing" DOUBLE PRECISION,
		"per_capita_income" DOUBLE PRECISION,
		"unemployment" DOUBLE PRECISION,
		"zip_code" VARCHAR(255),
		"api_endpoint" VARCHAR(255),
		PRIMARY KEY ("id") 
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		fmt.Println("Couldn't create table unemployment")
		writeToLog("Couldn't create table unemployment")
		return _err
	}

	writeToLog("Table unemployment created successfully")
	return nil
}

func unempExists(unemp string) bool {
	exists := false
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return exists
	}
	defer db.Close()

	sql := fmt.Sprintf(`SELECT "community_area" FROM "unemployment" where "community_area" = '%s'`, unemp)
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

func addUnempRecord(unemp unemployment) (bool, error) {
	db, err := common.OpenConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return false, err
	}
	defer db.Close()

	if unempExists(unemp.ComArea) {
		writeToLog("Skipping record %s. Already exists in database.", unemp.ComArea)
		return false, nil
	}

	sql := `INSERT INTO unemployment ("community_area",
									"community_area_name",
									"below_poverty_level",
									"crowded_housing",
									"per_capita_income",
									"unemployment",
									"zip_code",
									"api_endpoint") 
			values($1, $2, $3, $4, $5, $6, $7, $8)`

	// convert string to floats
	bpl, _ := strconv.ParseFloat(unemp.BelowPL, 64)
	ch, _ := strconv.ParseFloat(unemp.Crowded, 64)
	pci, _ := strconv.ParseFloat(unemp.PCIncome, 64)
	un, _ := strconv.ParseFloat(unemp.Unemployment, 64)

	_, err = db.Exec(sql,
		unemp.ComArea,
		unemp.CAName,
		bpl,
		ch,
		pci,
		un,
		unemp.ZipCode,
		unemp.API)

	if err != nil {
		writeToLog("Couldn't write community area-%s record to unemployment table", unemp.ComArea)
		return false, err
	}

	return true, nil
}
