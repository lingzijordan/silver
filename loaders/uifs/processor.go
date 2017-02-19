package uifs

import (
	"github.com/yewno/silver/services"
	"io/ioutil"
	"os"
	"encoding/csv"
	"github.com/yewno/log"
	"io"
	"github.com/yewno/silver/formats"
	"github.com/yewno/silver/utils"
	//"database/sql"
	//"sync"
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

	r := csv.NewReader(object.File)
	r.Comma = ','

	sourceInChan := make(chan *formats.UifsCode, 10000)
	//outChan := make(chan int, 4294967295)
	for {
		var code formats.UifsCode
		err := utils.CsvUnmarshal(r, &code)
		if err != nil {
			log.Debugf(err.Error())
		}
		if err == io.EOF {
			break
		}

		err = PullData(sctx.DB, &code, sctx)
		if err != nil {
			log.WithError(err)
		}
		log.Infof("%v", code)

		sourceInChan <- &code
	}
	close(sourceInChan)
	log.Infof("sourceInChan is %d", len(sourceInChan))

	//var w sync.WaitGroup
	//for i := 0; i < 3; i++ {
	//	w.Add(1)
	//	go func(id int, db *sql.DB, inChan chan *formats.UifsCode, sctx *services.ServiceContext, w *sync.WaitGroup) {
	//		defer w.Done()
	//		for code := range inChan {
	//
	//			err = PullData(db, code, sctx)
	//			if err != nil {
	//				log.WithError(err)
	//			}
	//		}
	//		log.Infof("finished worker %d", id)
	//	}(i, sctx.DB, sourceInChan, sctx, &w)
	//}
	//w.Wait()

	return nil
}
