package worldGovernance

import (
	"encoding/csv"
	"io"
	"sync"
	"database/sql"
	"io/ioutil"
	"os"
	"github.com/yewno/silver/formats"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/utils"
	"strings"
)

func Process(sctx *services.ServiceContext, key string,) error {
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

	sourceInChan := make(chan *formats.WorldGovernance, 4294967295)
	for {
		var record formats.WorldGovernance
		err := utils.CsvUnmarshal(r, &record)
		if err != nil {
			log.Debugf(err.Error())
		}
		if err == io.EOF {
			break
		}
		if strings.Contains(record.CountryName, "Country Name") {continue}

		log.Infof("%v", record)

		sourceInChan <- &record
	}
	close(sourceInChan)

	log.Infof("sourceInChan is %d", len(sourceInChan))

	var w sync.WaitGroup
	for i := 0; i < 50; i++ {
		w.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.WorldGovernance, table string, w *sync.WaitGroup) {
			defer w.Done()
			for record := range inChan {

				err = LoadData(db, record, table)
				if err != nil {
					log.WithError(err)
				}
			}
			log.Infof("finished worker %d", id)
		}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &w)
	}
	w.Wait()
	tx.Commit()

	return nil
}
