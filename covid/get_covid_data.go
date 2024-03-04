package covid

import (
	"fmt"
	"net/http"
	"p3-gcp/common"
	"sync"
)

func getCovid(done <-chan interface{}, urls ...string) <-chan common.Response {
	var wg sync.WaitGroup
	responses := make(chan common.Response)

	for _, url := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			fmt.Println("Making API call: ", url)
			writeToLog("Making API call: %s", url)
			resp, err := http.Get(url)
			select {
			case <-done:
				return
			case responses <- common.Response{URL: url, Error: err, Response: resp}:
			}
		}(url)
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	return responses
}
