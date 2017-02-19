package services

import (
	"strings"
	"net/http"
	"io/ioutil"
)

func Get(url string) ([]byte, error) {
	reader := strings.NewReader(`{"body":123}`)
	request, _ := http.NewRequest("GET", url, reader)
	// TODO: check err
	client := &http.Client{}
	resp, _ := client.Do(request)
	body := resp.Body
	return ioutil.ReadAll(body)
}
