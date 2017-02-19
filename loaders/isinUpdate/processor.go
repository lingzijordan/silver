package isinUpdate

import (
	"os"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	"bufio"
	"strings"
	"sync"
	"database/sql"
)

func Process(sctx *services.ServiceContext, key string) error {

	//tmpdir, err := ioutil.TempDir("/tmp", sctx.Cfg.Source)
	//if err != nil {
	//	log.WithError(err).Error("unable to make tmp dir")
	//	return err
	//}
	//defer os.RemoveAll(tmpdir)
	//
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

	tx, err := sctx.DB.Begin()
	if err != nil {
		log.WithError(err)
	}

	//_, err = sctx.DB.Exec(CreateTable(sctx.Cfg.DBtable), )
	//if err != nil {
	//	log.WithError(err)
	//}

	//file, _:= os.Open("/Users/ziling/Desktop/company_customer")

	sourceInChan := make(chan *formats.IsinUpdate, 2000)

	scanner := bufio.NewScanner(f)
	var counter int
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")
		log.Infof("%s", line)

		var record *formats.IsinUpdate

		if len(arr) == 2 {
			counter++
			record = &formats.IsinUpdate{
				Isin:           arr[0],
				Ticker:         arr[1],
			}

			sourceInChan <- record

		}

		if counter % 1000 == 0 {
			log.Infof("%d", counter)
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err.Error())
	}

	close(sourceInChan)

	log.Infof("sourceInChan is %d", len(sourceInChan))

	var w sync.WaitGroup
	for i := 0; i < 2; i++ {
		w.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.IsinUpdate, table string, w *sync.WaitGroup) {
			defer w.Done()
			for record := range inChan {

				err = UpdateData(db, record, table)
				if err != nil {
					log.WithError(err)
				}
			}
			log.Infof("finished worker %d", id)
		}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &w)
	}
	w.Wait()
	tx.Commit()

	//}
	return nil
}