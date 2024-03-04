package common

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/lib/pq"
)

var (
	logger *log.Logger
	mu     sync.Mutex
)

const (
	host     = "/cloudsql/msds432-cbi-phase3:us-central1:cbipostgres"
	port     = 5432
	user     = "postgres"
	password = "root"
	dbname   = "chicago_business_intelligence"
)

func OpenConnection() (*sql.DB, error) {
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

func databaseExists() (bool, error) {
	db, err := OpenConnection()
	if err != nil {
		return false, err
	}
	defer db.Close()

	var exists bool
	err = db.QueryRow("SELECT 1 FROM pg_database WHERE datname = $1", dbname).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func CreateDatabase() error {

	// Check if the database already exists
	exists, _ := databaseExists()
	if exists {
		fmt.Println("Database already exists")
		return nil
	}

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s sslmode=disable", host, port, user, password))
	if err != nil {
		fmt.Println("Failed to connect to postgres:", err)
		return err
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbname))
	if err != nil {
		fmt.Println("Couldn't create database")
		return err
	}

	return nil
}
