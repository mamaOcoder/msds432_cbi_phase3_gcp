package unemployment

type unemployment struct {
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
