package stockPrices

import (
	"os"
	//"encoding/csv"
	//"io"
	"sync"
	"database/sql"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	//"github.com/yewno/silver/utils"
	"path"
	"strings"
	//"fmt"
	"bufio"
	//"fmt"
	"fmt"
)

func Process(sctx *services.ServiceContext, key string) error {

	fname := path.Base(key)
	base := strings.Split(fname, ".")[0]

	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	tx, err := sctx.DB.Begin()

	_, err = sctx.DB.Exec(CreateTable(sctx.Cfg.DBtable), )
	if err != nil {
		log.WithError(err)
	}

	sourceInChan := make(chan *formats.StockPricesTable, 1000000)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		if len(arr) >= 6 {

			date := strings.TrimSpace(arr[0])
			if len(date) >= 8 {
				date = fmt.Sprintf("%s-%s-%s", date[:4], date[4:6], date[6:])
			}

			//if record.Ticker == "ticker" {continue}
			entry := &formats.StockPricesTable{
				Ticker: base,
				Date: date,
				Open: strings.TrimSpace(arr[1]),
				High: strings.TrimSpace(arr[2]),
				Low: strings.TrimSpace(arr[3]),
				Close: strings.TrimSpace(arr[4]),
				Volume: strings.TrimSpace(arr[5]),
			}

			sourceInChan <- entry
		}
		//fmt.Println("%v", entry)
	}
	close(sourceInChan)

	fmt.Println("%s %d", base, len(sourceInChan))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.StockPricesTable, table string, wg *sync.WaitGroup) {
			defer wg.Done()
			for record := range inChan {

				err = LoadData(db, record, table)
				if err != nil {
					log.WithError(err)
				}
			}
			//log.Infof("finished worker %d", id)
		}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &wg)
	}
	wg.Wait()

	tx.Commit()

	return nil
}
