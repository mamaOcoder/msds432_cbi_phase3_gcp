package ccvi

import "p3-gcp/common"

type ccvi struct {
	GeographyType string          `json:"geography_type"`
	CAorZip       string          `json:"community_area_or_zip"`
	CAName        string          `json:"community_area_name"`
	CcviScore     string          `json:"ccvi_score"`
	CcviCategory  string          `json:"ccvi_category"`
	Location      common.GeoCoord `json:"location"`
	ComArea       string
	ZipCode       string
	URL           string
	API           string
	ID            string
}
