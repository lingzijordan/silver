package unTrading

import (
	"database/sql"

	"github.com/yewno/silver/formats"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/utils"
	"io/ioutil"
	"os"
	"encoding/csv"
	"io"
	"strings"
	"sync"
)

func Process(sctx *services.ServiceContext, key string) error {
	isHS := strings.Contains(key, "HS")

	tmpdir, err := ioutil.TempDir("/tmp", sctx.Cfg.Source)
	if err != nil {
		log.WithError(err).Error("unable to make tmp dir")
		return err
	}
	defer os.RemoveAll(tmpdir)

	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return err
	}
	defer object.Close()

	r := csv.NewReader(object.File)
	r.Comma = ','

	tx, err := sctx.DB.Begin()

	_, err = sctx.DB.Exec(CreateTable(sctx.Cfg.DBtable), )
	if err != nil {
		log.WithError(err)
	}

	if isHS {

		sourceInChan := make(chan *formats.UnBulkHS, 4294967295)
		var counter int

		for {

			var unRecordHS formats.UnBulkHS
			err := utils.CsvUnmarshal(r, &unRecordHS)
			if err != nil {
				log.WithError(err)
			}
			if err == io.EOF {
				break
			}
			sourceInChan <- &unRecordHS
		}
		close(sourceInChan)

		log.Infof("size of sourceInChan %d", len(sourceInChan))

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int, db *sql.DB, inChan chan *formats.UnBulkHS, table string, wg *sync.WaitGroup, c int) {
				defer wg.Done()
				for record := range inChan {
					c++
					if c%10000 == 0 {
						log.Infof("counter %d", c)
					}

					err = LoadDataHS(db, record, table)
					if err != nil {
						log.WithError(err)
					}
				}
				log.Infof("finished worker %d", id)
			}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &wg, counter)
		}
		wg.Wait()

	} else {

		sourceInChan := make(chan *formats.UnBulk, 4294967295)
		//outChan := make(chan int, 4294967295)
		var counter int

		for {
			var unRecordEB02 formats.UnBulk
			err := utils.CsvUnmarshal(r, &unRecordEB02)
			if err != nil {
				log.WithError(err)
			}
			if err == io.EOF {
				break
			}

			sourceInChan <- &unRecordEB02
		}
		close(sourceInChan)
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int, db *sql.DB, inChan chan *formats.UnBulk, table string, wg *sync.WaitGroup, c int) {
				defer wg.Done()
				for record := range inChan {

					c++
					if c%10000 == 0 {
						log.Infof("counter %d", c)
					}
					err = LoadDataEB02(db, record, table)
					if err != nil {
						log.WithError(err)
					}
				}
				log.Infof("finished worker %d", id)
			}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &wg, counter)
		}
		wg.Wait()
	}

	tx.Commit()

	//}
	return nil
}


