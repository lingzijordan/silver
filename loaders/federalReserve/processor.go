package federalReserve

import (
	"database/sql"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	"encoding/json"
	"sync"
	"io/ioutil"
	"os"
)

func LoadCodes(file *os.File) *formats.SeriesId {
	//bytes, err := ioutil.ReadFile("/reference/federal-reserve-code.json")

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Debugf("reading file failed!")
	}
	codes := new(formats.SeriesId)
	if err = json.Unmarshal(bytes, codes); err != nil {
		log.WithError(err)
		return codes
	}
	return codes
}

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

	code := LoadCodes(object.File)

	sourceInChan := make(chan string, len(code.Code))
	for _, c := range code.Code {
		sourceInChan <- c.ID
	}
	close(sourceInChan)
	log.Infof("sourceInChan is %d", len(sourceInChan))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int, sctx *services.ServiceContext, db *sql.DB, inChan chan string, wg *sync.WaitGroup) {
			defer wg.Done()
			for code := range inChan {

				err := PullData(sctx, code)
				if err != nil {
					log.Debugf("Pulling series id: %s failed!", code)
				}
			}
			log.Infof("finished worker %d", id)
		}(i, sctx, sctx.DB, sourceInChan, &wg)
	}
	wg.Wait()

	return nil
}

