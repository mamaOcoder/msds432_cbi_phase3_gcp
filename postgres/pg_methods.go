package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"p3-docker/common"
	"strconv"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

var (
	logger *log.Logger
	mu     sync.Mutex
)

const (
	host     = "host.docker.internal"
	port     = 5433
	user     = "postgres"
	password = "root"
	dbname   = "chicago_business_intelligence"
)

func init() {
	// Open or create a log file
	currentTime := time.Now().Format("2006-01-02_15-04-05")
	logFilename := fmt.Sprintf("logfiles/postgres_%s.log", currentTime)
	file, err := os.OpenFile(logFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}

	// Create a logger that writes to the file
	logger = log.New(file, "", log.LstdFlags)
}

func writeToLog(format string, args ...interface{}) {
	// Use a mutex to ensure safe concurrent access to the log file
	mu.Lock()
	defer mu.Unlock()
	message := fmt.Sprintf(format, args...)
	logger.Println(message)
}

func openConnection() (*sql.DB, error) {
	// connection string
	conn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	// open database
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func CreateDatabase() error {

	// Check if the database exists
	db, err := openConnection()
	defer db.Close()
	if err == nil {
		fmt.Println("Database already exists")
		writeToLog("Database already exists")
		return nil
	}

	cmd := exec.Command("createdb", "-h", host, "-p", fmt.Sprintf("%d", port), "-U", user, "-W", dbname)
	if err := cmd.Run(); err != nil {
		fmt.Println("Couldn't create database")
		writeToLog("Couldn't create database")
		return err
	}

	writeToLog("Database created successfully.")

	return nil
}

// PostgreSQL code for transportation data
func CreateTaxiTable() error {
	db, err := openConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	// Install PostGIS extension
	//if _, err := db.Exec("CREATE EXTENSION IF NOT EXISTS postgis;"); err != nil {
	//	log.Fatal("Error installing PostGIS extension: ", err)
	//}

	// Enable PostGIS extension in the database
	//if _, err := db.Exec("CREATE EXTENSION IF NOT EXISTS postgis;"); err != nil {
	//	log.Fatal("Error enabling PostGIS extension: ", err)
	//}

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
		-- "pickup_centroid_location" GEOGRAPHY(Point, 4326),
		"dropoff_centroid_latitude" DOUBLE PRECISION, 
		"dropoff_centroid_longitude" DOUBLE PRECISION, 
		-- "dropoff_centroid_location" GEOGRAPHY(Point, 4326),
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
	db, err := openConnection()
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

func AddTaxiTrip(taxi common.TaxiTrip) error {
	db, err := openConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	if tripExists(taxi.TripID) {
		writeToLog("Skipping trip %s. Already exists in database.", taxi.TripID)
		return nil
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
									-- "pickup_centroid_location",
									"dropoff_centroid_latitude", 
									"dropoff_centroid_longitude", 
									-- "dropoff_centroid_location",
									"pickup_zip_code", 
									"dropoff_zip_code", 
									"api_endpoint") 
			values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)` //, $15, $16)`

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
		//pickupLoc,
		taxi.DropoffLatitude,
		taxi.DropoffLongitude,
		//dropoffLoc,
		taxi.PickupZipCode,
		taxi.DropoffZipCode,
		taxi.API)

	if err != nil {
		writeToLog("Couldn't write %s trip to taxi table", taxi.TripID)
		return err
	}

	return nil
}

// PostgreSQL code for Covid 19 Cases Data
func CreateCovidTable() error {
	db, err := openConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	// Install PostGIS extension
	//if _, err := db.Exec("CREATE EXTENSION IF NOT EXISTS postgis;"); err != nil {
	//	log.Fatal("Error installing PostGIS extension: ", err)
	//}

	// Enable PostGIS extension in the database
	//if _, err := db.Exec("CREATE EXTENSION IF NOT EXISTS postgis;"); err != nil {
	//	log.Fatal("Error enabling PostGIS extension: ", err)
	//}

	create_table := `CREATE TABLE IF NOT EXISTS "covid19_cases" (
		"row_id" VARCHAR(255) UNIQUE, 
		"zip_code" VARCHAR(255),
		"week_number" VARCHAR(255),
		"week_start" TIMESTAMP WITH TIME ZONE, 
		"week_end" TIMESTAMP WITH TIME ZONE, 
		"cases_weekly" INTEGER,
		-- "location" GEOGRAPHY(Point, 4326),
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
	db, err := openConnection()
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

func AddCovidRecord(covid common.CovidCases) error {
	db, err := openConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	if covidExists(covid.RowID) {
		writeToLog("Skipping record %s. Already exists in database.", covid.RowID)
		return nil
	}

	sql := `INSERT INTO covid19_cases ("row_id", 
										"zip_code",
										"week_number",
										"week_start", 
										"week_end", 
										"cases_weekly",
										-- "location",
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
		//loc,
		lat,
		lon,
		covid.ComArea,
		covid.API)

	if err != nil {
		writeToLog("Couldn't write %s record to covid19 table", covid.RowID)
		return err
	}

	return nil
}

// PostgreSQL code for CCVI Data
func CreateCcviTable() error {
	db, err := openConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	// Install PostGIS extension
	//if _, err := db.Exec("CREATE EXTENSION IF NOT EXISTS postgis;"); err != nil {
	//	log.Fatal("Error installing PostGIS extension: ", err)
	//}

	// Enable PostGIS extension in the database
	//if _, err := db.Exec("CREATE EXTENSION IF NOT EXISTS postgis;"); err != nil {
	//	log.Fatal("Error enabling PostGIS extension: ", err)
	//}

	create_table := `CREATE TABLE IF NOT EXISTS "ccvi" (
		"id" VARCHAR(255) UNIQUE,
		"geography_type" VARCHAR(255), 
		"community_area" VARCHAR(255),
		"community_area_name" VARCHAR(255),
		"zip_code" VARCHAR(255),
		"ccvi_category" VARCHAR(255),
		"ccvi_score" DOUBLE PRECISION,
		-- "location" GEOGRAPHY(Point, 4326),
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
	db, err := openConnection()
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

func AddCcviRecord(ccvi common.CCVI) error {
	db, err := openConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	if covidExists(ccvi.ID) {
		writeToLog("Skipping record %s. Already exists in database.", ccvi.ID)
		return nil
	}

	sql := `INSERT INTO ccvi ("id", 
							"geography_type",
							"community_area",
							"community_area_name", 
							"zip_code", 
							"ccvi_category",
							"ccvi_score",
							-- "location",
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
		//loc,
		lat,
		lon,
		ccvi.API)

	if err != nil {
		writeToLog("Couldn't write %s record to ccvi table", ccvi.ID)
		return err
	}

	return nil
}

// PostgreSQL code for Unemployment Data
func CreateUnemploymentTable() error {
	db, err := openConnection()
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
	db, err := openConnection()
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

func AddUnempRecord(unemp common.Unemployment) error {
	db, err := openConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	if covidExists(unemp.ComArea) {
		writeToLog("Skipping record %s. Already exists in database.", unemp.ComArea)
		return nil
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
		return err
	}

	return nil
}

// PostgreSQL code for Unemployment Data
func CreatePermitTable() error {
	db, err := openConnection()
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
		"work_description" VARCHAR(255),
		"total_fee" DOUBLE PRECISION,
		"reported_cost" DOUBLE PRECISION,
		"community_area" VARCHAR(255),
		"xcoordinate" DOUBLE PRECISION,
		"ycoordinate" DOUBLE PRECISION,
		"latitude" DOUBLE PRECISION,
		"longitude" DOUBLE PRECISION,
		-- "location" GEOGRAPHY(Point, 4326),
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
	db, err := openConnection()
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

func AddPermitRecord(permit common.BuildingPermit) error {
	db, err := openConnection()
	if err != nil {
		fmt.Println("Couldn't connect to database")
		writeToLog("Couldn't connect to database")
		return err
	}
	defer db.Close()

	if covidExists(permit.ID) {
		writeToLog("Skipping record %s. Already exists in database.", permit.ID)
		return nil
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
										-- "location",
										"zip_code",
										"api_endpoint") 
			values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)` //, $16)`

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
		//loc,
		permit.ZipCode,
		permit.API)

	if err != nil {
		writeToLog("Couldn't write %s record to building_permit table", permit.ID)
		return err
	}

	return nil
}
