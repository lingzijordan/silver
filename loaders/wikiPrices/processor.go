package wikiPrices

import (
	"io"
	"sync"
	"database/sql"
	"os"
	"encoding/csv"
	"github.com/yewno/log"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/formats"
	"github.com/yewno/silver/utils"
)

func Process(sctx *services.ServiceContext, key string) error {
	//tmpdir, err := ioutil.TempDir("/tmp", sctx.Cfg.Source)
	//if err != nil {
	//	log.WithError(err).Error("unable to make tmp dir")
	//	return err
	//}
	//defer os.RemoveAll(tmpdir)

	//object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	//if err := sctx.Storage.Get(object); err != nil {
	//	log.WithError(err).Error("")
	//	return err
	//}
	//defer object.Close()

	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	r := csv.NewReader(f) //object.File
	r.Comma = ','

	tx, err := sctx.DB.Begin()

	_, err = sctx.DB.Exec(CreateTable(sctx.Cfg.DBtable), )
	if err != nil {
		log.WithError(err)
	}

	sourceInChan := make(chan *formats.WikiPrices, 4294967295)
	for {
		var record formats.WikiPrices
		err := utils.CsvUnmarshal(r, &record)
		if err != nil {
			log.WithError(err)
		}
		if err == io.EOF {
			break
		}
		if record.Ticker == "ticker" {continue}

		sourceInChan <- &record
	}
	close(sourceInChan)

	log.Infof("sourceInChan is %d", len(sourceInChan))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.WikiPrices, table string, wg *sync.WaitGroup) {
			defer wg.Done()
			for record := range inChan {

				err = LoadData(db, record, table)
				if err != nil {
					log.WithError(err)
				}
			}
			log.Infof("finished worker %d", id)
		}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &wg)
	}
	wg.Wait()

	tx.Commit()

	return nil
}
