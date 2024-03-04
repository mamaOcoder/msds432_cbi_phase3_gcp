package unemployment

import (
	"fmt"
	"net/http"
	"p3-gcp/common"
)

func getUnemployment(url string) common.Response {

	fmt.Println("Making API call: ", url)
	writeToLog("Making API call: %s", url)
	resp, err := http.Get(url)
	unempResponse := common.Response{URL: url, Error: err, Response: resp}

	return unempResponse
}
