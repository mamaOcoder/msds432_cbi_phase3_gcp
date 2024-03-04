package ccvi

import (
	"fmt"
	"net/http"
	"p3-gcp/common"
)

func getCCVI(url string) common.Response {

	fmt.Println("Making API call: ", url)
	writeToLog("Making API call: %s", url)
	resp, err := http.Get(url)
	ccviResponse := common.Response{URL: url, Error: err, Response: resp}

	return ccviResponse
}
