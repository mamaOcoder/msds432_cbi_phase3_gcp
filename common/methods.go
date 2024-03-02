package common

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelvins/geocoder"
)

// Function to build out the API url with a limit and pagination info
// We will call the API for chunks of 1000 records
func BuildUrls(base_urls []string, limit int) []string {
	var query_urls []string

	url_limit := 1000
	if limit <= url_limit {
		url_limit = limit
	}

	for _, url := range base_urls {
		completed := 0
		page := 0
		for completed < limit {
			taxi_url := fmt.Sprintf("%s?$limit=%d&$offset=%d", url, url_limit, page*url_limit)

			// If overall limit is not divisible by 1000, then the final call will have a limit less than 1000
			remain := limit - completed
			if remain < url_limit {
				taxi_url = fmt.Sprintf("%s?$limit=%d&$offset=%d", url, remain, page*url_limit)
			}

			query_urls = append(query_urls, taxi_url)
			page++
			completed += url_limit
		}

	}

	return query_urls
}

func CheckTimeFormat(timeString string) bool {
	_, err := time.Parse("2006-01-02T15:04:05.000", timeString)
	return err == nil
}

func ExtractAPI(urlString string) (string, error) {
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		return "", err
	}

	pathComponents := strings.Split(parsedURL.Path, "/")
	apiEndpoint := pathComponents[len(pathComponents)-1]

	return apiEndpoint, nil
}

func CaFromGeo(geo []float64) (string, error) {
	// Load API key
	err := godotenv.Load()
	if err != nil {
		log.Fatal(".env file with API key could not be loaded")
	}
	geocoder.ApiKey = os.Getenv("API_KEY")

	location := geocoder.Location{
		Latitude:  geo[1],
		Longitude: geo[0],
	}
	address_list, _ := geocoder.GeocodingReverse(location)
	com_area := address_list[0].Neighborhood

	if com_area == "" {
		com_area = "None"
	}

	return com_area, nil
}
