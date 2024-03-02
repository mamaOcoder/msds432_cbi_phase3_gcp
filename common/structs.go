package common

import "net/http"

// This package defines structs that are used in multiple packages
type Response struct {
	URL      string
	Error    error
	Response *http.Response
}

type geoCoord struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

type TaxiTrip struct {
	TripID           string   `json:"trip_id"`
	TaxiID           string   `json:"taxi_id"`
	TripStartTime    string   `json:"trip_start_timestamp"`
	TripEndTime      string   `json:"trip_end_timestamp"`
	TripDuration     string   `json:"trip_seconds"`
	TripMiles        string   `json:"trip_miles"`
	PickupCA         string   `json:"pickup_community_area"`
	DropoffCA        string   `json:"dropoff_community_area"`
	Fare             string   `json:"fare"`
	Tips             string   `json:"tips"`
	Tolls            string   `json:"tolls"`
	Extra            string   `json:"extras"`
	TripTotal        string   `json:"trip_total"`
	PaymentType      string   `json:"payment_type"`
	Company          string   `json:"company"`
	PickupLatitude   string   `json:"pickup_centroid_latitude"`
	PickupLongitude  string   `json:"pickup_centroid_longitude"`
	PickupLocation   geoCoord `json:"pickup_centroid_location"`
	DropoffLatitude  string   `json:"dropoff_centroid_latitude"`
	DropoffLongitude string   `json:"dropoff_centroid_longitude"`
	DropoffLocation  geoCoord `json:"dropoff_centroid_location"`
	ComputedRegion   string   `json:"@computed_region_vrxf_vc4k"`
	PickupZipCode    string
	DropoffZipCode   string
	URL              string
	API              string
}

type CovidCases struct {
	RowID       string   `json:"row_id"`
	ZipCode     string   `json:"zip_code"`
	WeekNum     string   `json:"week_number"`
	WeekStart   string   `json:"week_start"`
	WeekEnd     string   `json:"week_end"`
	CasesWeekly string   `json:"cases_weekly"`
	Location    geoCoord `json:"zip_code_location"`
	ComArea     string
	URL         string
	API         string
}

type CCVI struct {
	GeographyType string   `json:"geography_type"`
	CAorZip       string   `json:"community_area_or_zip"`
	CAName        string   `json:"community_area_name"`
	CcviScore     string   `json:"ccvi_score"`
	CcviCategory  string   `json:"ccvi_category"`
	Location      geoCoord `json:"location"`
	ComArea       string
	ZipCode       string
	URL           string
	API           string
	ID            string
}

type Unemployment struct {
	ComArea      string `json:"community_area"`
	CAName       string `json:"community_area_name"`
	BelowPL      string `json:"below_poverty_level"`
	Crowded      string `json:"crowded_housing"`
	PCIncome     string `json:"per_capita_income"`
	Unemployment string `json:"unemployment"`
	ZipCode      string
	URL          string
	API          string
}

type BuildingPermit struct {
	ID           string   `json:"id"`
	PermitNum    string   `json:"permit_"`
	PermitType   string   `json:"permit_type"`
	AppDate      string   `json:"application_start_date"`
	IssueDate    string   `json:"issue_date"`
	WorkDesc     string   `json:"work_description"`
	TotalFee     string   `json:"total_fee"`
	ReportedCost string   `json:"reported_cost"`
	ComArea      string   `json:"community_area"`
	XCoord       string   `json:"xcoordinate"`
	YCoord       string   `json:"ycoordinate"`
	Latitude     string   `json:"latitude"`
	Longitude    string   `json:"longitude"`
	Location     geoCoord `json:"location"`
	ZipCode      string
	URL          string
	API          string
}
