package common

import "net/http"

// This package defines structs that are used in multiple packages
type Response struct {
	URL      string
	Error    error
	Response *http.Response
}

type GeoCoord struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}
