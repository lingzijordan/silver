package main

import (
	"time"
	"strings"
	"sync"
	"os"
	"github.com/yewno/log"
	"github.com/yewno/log/handlers/text"
	"github.com/yewno/silver/config"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/utils"
	"path"
	"fmt"
	"errors"
	"io/ioutil"
	"bytes"
)

func main() {
	log.SetHandler(text.Default)
	log.SetLevel(log.DebugLevel)

	cfg := &config.Config{
		Bucket:          "yewno-finance",
		Credentials:     services.NewCredentials(),
		Region:          "us-west-2",
		ConfigTbl:       "silverDBConfig",
		StatsTbl:        "",
		DBtable:         "",
		Source:          "sec-gov",
		DBType:          "postgres",
		ProcessedQueue:  "",
	}

	dbcred := &config.DBcredentials{
		Database:        "finance_testing",
	}
	err := services.GetDBCredentials(cfg, dbcred)
	if err != nil {
		log.Debugf(err.Error())
	}

	if dbcred.Ip == "localhost" {
		dbcred.Ip = ""
	}

	sctx, err := services.NewServiceContext(cfg, dbcred)
	if err != nil {
		log.Debugf(err.Error())
	}
	//defer sctx.DB.Close()

	sourceInChan := make(chan string, 5000000)
	sourceOutChan := make(chan string, 5000000)
	keys := utils.RetrieveKeys(sctx, 100 * 24 * time.Hour)

	for _, key := range keys {
		if strings.Contains(key, ".txt") {
			sourceInChan <- key
		}
	}
	close(sourceInChan)

	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(id int, sctx *services.ServiceContext, inChan, outChan chan string, wg *sync.WaitGroup) {
			defer wg.Done()
			for key := range inChan {
				yid, err := check10KQ(sctx, key)
				if err != nil {
					log.WithError(err)
					continue
				}
				outChan <- yid
			}
			//log.Infof("finished worker %d", id)
		}(i, sctx, sourceInChan, sourceOutChan, &wg)
	}
	wg.Wait()
	close(sourceOutChan)

	flagFile := "/Users/ziling/yid_10KQ.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for k := range sourceOutChan {
		k = fmt.Sprintf("%s\n", k)
		_, err := file.WriteString(k)
		if err != nil {
			log.WithError(err)
			continue
		}
	}

	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}
}

func check10KQ(sctx *services.ServiceContext, key string) (string, error) {
	yid := strings.Split(path.Base(key), ".")[0]

	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return "", err
	}
	defer object.Close()

	bytesArr, err := ioutil.ReadAll(object.File)
	if err != nil {
		log.WithError(err)
	}

	if bytes.Contains(bytesArr, []byte("10-Q")) || bytes.Contains(bytesArr, []byte("10-K")) {
		log.Infof(yid)
		return yid, nil
	}

	return "", errors.New("Not 10-Q/K file!")
}