package covid

import "p3-gcp/common"

type covidCases struct {
	RowID       string          `json:"row_id"`
	ZipCode     string          `json:"zip_code"`
	WeekNum     string          `json:"week_number"`
	WeekStart   string          `json:"week_start"`
	WeekEnd     string          `json:"week_end"`
	CasesWeekly string          `json:"cases_weekly"`
	Location    common.GeoCoord `json:"zip_code_location"`
	ComArea     string
	URL         string
	API         string
}
