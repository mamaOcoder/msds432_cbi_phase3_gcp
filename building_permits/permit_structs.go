package building_permits

import "p3-gcp/common"

type buildingPermit struct {
	ID           string          `json:"id"`
	PermitNum    string          `json:"permit_"`
	PermitType   string          `json:"permit_type"`
	AppDate      string          `json:"application_start_date"`
	IssueDate    string          `json:"issue_date"`
	WorkDesc     string          `json:"work_description"`
	TotalFee     string          `json:"total_fee"`
	ReportedCost string          `json:"reported_cost"`
	ComArea      string          `json:"community_area"`
	XCoord       string          `json:"xcoordinate"`
	YCoord       string          `json:"ycoordinate"`
	Latitude     string          `json:"latitude"`
	Longitude    string          `json:"longitude"`
	Location     common.GeoCoord `json:"location"`
	ZipCode      string
	URL          string
	API          string
}
