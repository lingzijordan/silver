package main

import (
	"time"
	"os"
	"sync"
	"strings"
	"fmt"
	"github.com/yewno/log"
	"github.com/yewno/log/handlers/text"
	"github.com/yewno/silver/config"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/utils"
	"encoding/json"
	"io/ioutil"
	"path"
)

type Meta struct {
	YID        string `json:"yId"`
	Created    string `json:"created"`
	Date       time.Time `json:"date"`
	Day        int `json:"day"`
	Month      int `json:"month"`
	Year       int `json:"year"`
	Language   string `json:"language"`
	Headline   string `json:"headline"`
	Type       string `json:"type"`
	IngestedAt string `json:"ingestedAt"`
	Source     string `json:"source"`
	Tickers    string `json:"tickers"`
}

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
		Source:          "tr-news",
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
		if strings.Contains(key, ".json") {
			sourceInChan <- key
		}
	}
	close(sourceInChan)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int, sctx *services.ServiceContext, inChan, outChan chan string, wg *sync.WaitGroup) {
			defer wg.Done()
			for key := range inChan {
				item, err := extractMeta(sctx, key)
				if err != nil {
					log.WithError(err)
					continue
				}
				outChan <- item
			}
			//log.Infof("finished worker %d", id)
		}(i, sctx, sourceInChan, sourceOutChan, &wg)
	}
	wg.Wait()
	close(sourceOutChan)

	flagFile := "/Users/ziling/trnews_list.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for k := range sourceOutChan {
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

func extractMeta(sctx *services.ServiceContext, key string) (string, error) {
	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return "", err
	}
	defer object.Close()

	response, err := ioutil.ReadAll(object.File)
	if err != nil {
		log.WithError(err)
		return "", err
	}
	meta := new(Meta)
	if err = json.Unmarshal(response, meta); err != nil {
		log.WithError(err)
		return "", err
	}

	yid := strings.Split(path.Base(key), ".")[0]
	s := fmt.Sprintf("%d,%d,%d,%s\n", meta.Year, meta.Month, meta.Day, yid)

	return s, nil
}
