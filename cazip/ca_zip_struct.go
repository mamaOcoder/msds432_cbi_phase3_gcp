package cazip

import "github.com/paulmach/orb"

type Geometry struct {
	Type        string           `json:"type"`
	Coordinates orb.MultiPolygon `json:"coordinates"`
}

// Implement the Bound method for Geometry
func (g Geometry) Bound() orb.Bound {
	return g.Coordinates.Bound()
}

type caLookup struct {
	ComArea  string   `json:"area_numbe"`
	CAName   string   `json:"community"`
	Geometry Geometry `json:"the_geom"`
}

type zipLookup struct {
	Geometry Geometry `json:"the_geom"`
	ZipCode  string   `json:"zip"`
}

type caZipLookup struct {
	ComArea  string
	CAName   string
	Geometry Geometry
	ZipCode  string
}

type CALookupList []caLookup

var CALUList CALookupList

type ZipLookupList []zipLookup

var ZipLUList ZipLookupList
