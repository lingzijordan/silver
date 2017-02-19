package taxonomySp

import (
	"sync"
	"database/sql"
	"os"
	"bufio"
	"strings"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
	"github.com/yewno/silver/services"
	"io/ioutil"
)

func Process(sctx *services.ServiceContext, key string) error {

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

	tx, err := sctx.DB.Begin()

	_, err = sctx.DB.Exec(CreateTable(sctx.Cfg.DBtable), )
	if err != nil {
		log.WithError(err)
	}

	//file, _:= os.Open("/Users/ziling/Desktop/sp500")

	sourceInChan := make(chan *formats.SP500, 4294967295)

	scanner := bufio.NewScanner(object.File)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")

		record := &formats.SP500{
			Ticker:               arr[0],
			CompanyName:          arr[1],
			Isin:                 arr[2],
			Cusip:                arr[3],
		}
		sourceInChan <- record

		//fmt.Println(record)

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err.Error())
	}


	close(sourceInChan)

	log.Infof("sourceInChan is %d", len(sourceInChan))

	var w sync.WaitGroup
	for i := 0; i < 50; i++ {
		w.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.SP500, table string, w *sync.WaitGroup) {
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

	//}
	return nil
}
