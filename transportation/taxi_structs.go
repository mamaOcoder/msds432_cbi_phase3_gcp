package transportation

import "p3-gcp/common"

type taxiTrip struct {
	TripID           string          `json:"trip_id"`
	TaxiID           string          `json:"taxi_id"`
	TripStartTime    string          `json:"trip_start_timestamp"`
	TripEndTime      string          `json:"trip_end_timestamp"`
	TripDuration     string          `json:"trip_seconds"`
	TripMiles        string          `json:"trip_miles"`
	PickupCA         string          `json:"pickup_community_area"`
	DropoffCA        string          `json:"dropoff_community_area"`
	Fare             string          `json:"fare"`
	Tips             string          `json:"tips"`
	Tolls            string          `json:"tolls"`
	Extra            string          `json:"extras"`
	TripTotal        string          `json:"trip_total"`
	PaymentType      string          `json:"payment_type"`
	Company          string          `json:"company"`
	PickupLatitude   string          `json:"pickup_centroid_latitude"`
	PickupLongitude  string          `json:"pickup_centroid_longitude"`
	PickupLocation   common.GeoCoord `json:"pickup_centroid_location"`
	DropoffLatitude  string          `json:"dropoff_centroid_latitude"`
	DropoffLongitude string          `json:"dropoff_centroid_longitude"`
	DropoffLocation  common.GeoCoord `json:"dropoff_centroid_location"`
	ComputedRegion   string          `json:"@computed_region_vrxf_vc4k"`
	PickupZipCode    string
	DropoffZipCode   string
	URL              string
	API              string
}
