package main

import (
	"io/ioutil"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
)

func LoadCodes() *formats.ReportingArea {
	bytes, err := ioutil.ReadFile("/Users/ziling/Yewno/github.com/yewno/silver/reference/reporting-area.json")
	if err != nil {
		log.Debugf("reading file failed!")
	}
	codes := new(formats.ReportingArea)
	if err = json.Unmarshal(bytes, codes); err != nil {
		log.WithError(err)
		return codes
	}
	return codes
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	token := "4xemla6rLBcR5GJICv3zZT7/WR67/Eq4VFb/f3xaug3QvgDqPOOA3JXSJIPy8rdVk6pzBto18zXK9XPn+s2pfkYO+ZDc1h8b8+PskNnslqaQCycp/eJ98XhN2dpFx8Alxj+L5hcxRhCClf9eMlp6NA=="
	urlPrefix := "http://comtrade.un.org/api/get/bulk/"
	types := []string{"S"} //, "C"
	freq := []string{"A"}  //M
	ps := []string{"2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016"}
	px := []string{"EB02"} //"HS"
	//r := LoadCodes()

	urls := []string{}

	for _, ty := range types {
		for _, fre := range freq {
			for _, p := range ps {
				for _, x := range px {
					url := fmt.Sprintf("%s%s/%s/%s/842/%s?token=%s", urlPrefix, ty, fre, p, x, token)
					urls = append(urls, url)
					fmt.Printf("%s\n", url)
				}
			}
		}
	}

	sourceInChan := make(chan string, len(urls))
	for _, s := range urls {
		sourceInChan <- s
	}
	close(sourceInChan)
	var wg sync.WaitGroup
	for i:=0; i<50; i++ {
		wg.Add(1)

		go func(id int, inChan chan string, wg *sync.WaitGroup) {
			defer wg.Done()

			for link := range sourceInChan {

				s := strings.Split(link, "/")
				s2 := strings.Join(s[5:11], "-")
				s3 := strings.Split(s2, "?")[0]

				response, err := http.Get(link)
				check(err)
				body := response.Body
				if body == nil {
					fmt.Printf("nothing")
					return
				}
				bytes, err := ioutil.ReadAll(body)
				check(err)

				filePath := fmt.Sprintf("/Users/ziling/unusa/%s.csv.zip", s3)
				splitFile, err := os.OpenFile(filePath, os.O_RDWR | os.O_CREATE, 0644)
				if _, err := splitFile.Write(bytes); err != nil {
					log.WithError(err).Error("")
				}
				fmt.Printf("writing %s successfully!\n", filePath)
				if _, err := splitFile.Seek(0, 0); err != nil {
					log.WithError(err).Error("")
				}
			}
			fmt.Printf("finished worker %d\n", id)
		}(i, sourceInChan, &wg)
	}
	wg.Wait()
}