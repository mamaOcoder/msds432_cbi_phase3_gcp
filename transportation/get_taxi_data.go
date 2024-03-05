package transportation

import (
	"fmt"
	"net"
	"net/http"
	"p3-gcp/common"
	"sync"
	"time"
)

func getTaxi(done <-chan interface{}, urls ...string) <-chan common.Response {

	tr := &http.Transport{
		MaxIdleConns:          10,
		IdleConnTimeout:       1000 * time.Second,
		TLSHandshakeTimeout:   1000 * time.Second,
		ExpectContinueTimeout: 1000 * time.Second,
		DisableCompression:    true,
		Dial: (&net.Dialer{
			Timeout:   1000 * time.Second,
			KeepAlive: 1000 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: 1000 * time.Second,
	}

	client := &http.Client{Transport: tr}
	var wg sync.WaitGroup
	responses := make(chan common.Response)

	for _, url := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			fmt.Println("Making API call: ", url)
			writeToLog("Making API call: %s", url)
			//resp, err := http.Get(url)
			resp, err := client.Get(url)
			if err != nil {
				panic(err)
			}
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
