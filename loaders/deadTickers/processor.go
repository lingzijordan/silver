package deadTickers

import (
	"os"
	"sync"
	"database/sql"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
	"github.com/yewno/silver/services"
	"bufio"
	"strings"
)

func LoadCodes(file *os.File) chan *formats.Tickers {

	sourceInChan := make(chan *formats.Tickers, 1000)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		record := &formats.Tickers{
			Id:        arr[0],
			Ticker:    arr[1],
		}
		sourceInChan <- record
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err.Error())
	}

	close(sourceInChan)
	return sourceInChan
}

func Process(sctx *services.ServiceContext, key string) error {

	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return err
	}
	defer object.Close()

	tickers := LoadCodes(object.File)
	outChan := make(chan string, 1000)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int, sctx *services.ServiceContext, db *sql.DB, inChan chan *formats.Tickers, wg *sync.WaitGroup) {
			defer wg.Done()
			for ticker := range tickers {

				good, err := PullData(sctx, ticker.Ticker)
				if err != nil {
					log.Debugf("Pulling series id: %s failed!", ticker.Ticker)
				}
				if good {
					outChan <- ticker.Ticker
				}
			}
			log.Infof("finished worker %d", id)
		}(i, sctx, sctx.DB, tickers, &wg)
	}
	wg.Wait()
	close(outChan)
	log.Infof("tickers added number: %d", len(outChan))

	err := LoadRecords(sctx, outChan)
	if err != nil {
		log.Debugf(err.Error())
	}

	//for ticker := range outChan {
	//	err := RemoveRecords(sctx, ticker)
	//	if err != nil {
	//		log.Debugf(err.Error())
	//	}
	//	log.Infof("ticker %s being removed! ", ticker)
	//
	//	//err := LoadRecords(sctx, ticker)
	//	//if err != nil {
	//	//	log.Debugf(err.Error())
	//	//}
	//	//log.Infof("ticker %s being loaded! ", ticker)
	//}

	return nil
}
