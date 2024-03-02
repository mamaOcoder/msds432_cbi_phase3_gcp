package cazip

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func getCALookup(caChan chan<- []caLookup, errChan chan<- error) {
	defer close(caChan)

	var ca_url = "https://data.cityofchicago.org/resource/igwz-8jzy.json"

	// GET request to taxi API
	caResponse, err := http.Get(ca_url)
	if err != nil {
		errChan <- err
		return
	}
	defer caResponse.Body.Close()

	var returnCA []caLookup

	b, err := io.ReadAll(caResponse.Body)
	if err != nil {
		errChan <- err
		return
	}
	json.Unmarshal(b, &returnCA)

	caChan <- returnCA
}

func getZipLookup(zipChan chan<- []zipLookup, errChan chan<- error) {
	defer close(zipChan)

	var zip_url = "https://data.cityofchicago.org/resource/unjd-c2ca.json"

	// GET request to taxi API
	zipResponse, err := http.Get(zip_url)
	if err != nil {
		errChan <- err
		return
	}
	defer zipResponse.Body.Close()

	var returnZip []zipLookup

	b, err := io.ReadAll(zipResponse.Body)
	if err != nil {
		errChan <- err
		return
	}
	json.Unmarshal(b, &returnZip)

	zipChan <- returnZip
}

func GetCaLookupList() []caLookup {
	return CALUList
}

func GetZipLookupList() []zipLookup {
	return ZipLUList
}

func BuildLookups() {
	caChan := make(chan []caLookup)
	zipChan := make(chan []zipLookup)
	errChan := make(chan error)

	go getCALookup(caChan, errChan)
	go getZipLookup(zipChan, errChan)

	CALUList = <-caChan
	ZipLUList = <-zipChan

	close(errChan)

	if err, ok := <-errChan; ok {
		fmt.Println("Error:", err)
	}

	/*var lookup []caZipLookup

	for _, ca := range caLookup {
		multi := 0
		for _, zip := range zipLookup {
			if ca.Geometry.Coordinates.Equal(zip.Geometry.Coordinates) {
				lookup = append(lookup, caZipLookup{
					ComArea:  ca.ComArea,
					CAName:   ca.CAName,
					Geometry: ca.Geometry,
					ZipCode:  zip.ZipCode,
				})
				//break // Exit inner loop once a match is found
				multi++
			}

		}
		if multi > 1 {
			fmt.Printf("Found multiple zip codes for community area %s\n", ca.CAName)
		}
	}

	fmt.Println(lookup[0])
	//fmt.Println(caLookup[0])

	//fmt.Println(caLookup[0])
	//fmt.Println(zipLookup[0].ZipCode)

	//merge lookups based on the multipolygon field*/
}
