package sec

import (
	"os"
	"bufio"
	"fmt"
	"sync"
	"database/sql"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
)

func Process(sctx *services.ServiceContext, key string) error {

	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	sourceInChan := make(chan string, 2000)
	sourceOutChan := make(chan *formats.CikMapping, 2000)
	wrongChan := make(chan string, 2000)
	for scanner.Scan() {
		ticker := scanner.Text()

		sourceInChan <- ticker
		//         fmt.Println("%v", ticker)
	}
	close(sourceInChan)

	fmt.Println("%d", len(sourceInChan))

	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func(id int, db *sql.DB, inChan chan string, outChan chan *formats.CikMapping, wrongChan chan string, wg *sync.WaitGroup) {
			defer wg.Done()
			for ticker := range inChan {

				cik, err := GetCik(sctx, ticker)
				if err != nil {
					log.Debugf(err.Error())
					log.Debugf(ticker)
					wrongChan <- ticker
					continue
				}
				log.Infof("cik is %s", cik)
				mapping := &formats.CikMapping{
					Cik: cik,
					Ticker: ticker,
				}

				outChan <- mapping
			}
		}(i, sctx.DB, sourceInChan, sourceOutChan, wrongChan, &wg)
	}
	wg.Wait()
	close(sourceOutChan)
	close(wrongChan)

	flagFile := "/Users/ziling/cikTicker9.txt"
	file, err := os.OpenFile(flagFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for v := range sourceOutChan {
		s := fmt.Sprintf("%s,%s\n", v.Cik, v.Ticker)
		_, err := file.WriteString(s)
		if err != nil {
			log.WithError(err)
			continue
		}
	}

	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}

	flagFile2 := "/Users/ziling/cikNotThere9.txt"
	file2, err := os.OpenFile(flagFile2, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file2.Close()

	for v := range wrongChan {
		s := fmt.Sprintf("%s\n", v)
		_, err := file2.WriteString(s)
		if err != nil {
			log.WithError(err)
			continue
		}
	}

	if _, err := file2.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}

	return nil
}