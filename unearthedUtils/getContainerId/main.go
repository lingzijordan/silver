package main

import (
	"os"
	"bufio"
	"sync"
	"fmt"
	"time"
	"flag"
	"github.com/davecgh/go-spew/spew"
	"github.com/yewno/log"
	"github.com/yewno/log/handlers/text"
	"github.com/yewno/carbon/aws"
	"encoding/json"
	"github.com/yewno/silver/config"
	"github.com/yewno/silver/services"
	"strings"
)

var (
	flagDB string
	flagTableName string
	flagBucket string
	flagAWSRegion string
	flagDBConfigTable string
	flagStatsTable string
	flagSource string
	flagDBName string
	flagQueue string
	flagESHost string
	flagTimeFrame time.Duration
)

func init() {

	flag.StringVar(&flagDB, "database-type", "postgres", "choose which database to use")
	flag.StringVar(&flagTableName, "table", "", "which table to insert records")
	flag.StringVar(&flagBucket, "bucket", "yewno-finance", "which bucket to save csv files")
	flag.StringVar(&flagAWSRegion, "region", "us-west-2", "region")
	flag.StringVar(&flagDBConfigTable, "config", "silverDBConfig", "silver config table")
	flag.StringVar(&flagStatsTable, "stats", "", "cobalt stats table")
	flag.StringVar(&flagSource, "source", "", "data source to be ingested")
	flag.StringVar(&flagDBName, "database-name", "finance_testing", "which database to put table")
	flag.StringVar(&flagQueue, "processed-queue", "yewno-indexing-finance", "queue where pairs to sent for ingestion")
	flag.DurationVar(&flagTimeFrame, "duration", 100 * 24 * time.Hour, "how far back to search")
	flag.StringVar(&flagESHost, "eshost", "http://elasticsearch.yewno.io:9200/", "elastic search host address")

	log.SetHandler(text.Default)
	flag.Parse()
}

type YidContainerId struct {
	YID         string `json:"yid"`
	ContainerId string `json:"containerId,omitempty"`
	Chapter     interface{} `json:"chapter,omitempty"`
}

type Pair struct {
	Yid         string
	ContainerId string
	Chapter     interface{}
}

func getExisting(yid string) (*YidContainerId, error) {
	var doc YidContainerId
	dynamo, err := aws.GetDynamo()
	if err != nil {
		log.WithError(err).Error("")
		return &doc, err
	}
	ok, fullRecord, err := dynamo.GetRecord("contentMeta", "yId", yid)
	if err != nil {
		log.WithError(err).Error("")
		return &doc, err
	}
	if ok {
		bytesArr, err := json.Marshal(fullRecord)
		if err != nil {
			log.WithError(err).Error("")
			return &doc, err
		}
		if err := json.Unmarshal(bytesArr, &doc); err != nil {
			spew.Dump(fullRecord)
			log.WithError(err).Error("")
			return &doc, err
		}
	}
	return &doc, nil
}

func main() {
	log.SetLevel(log.DebugLevel)

	cfg := &config.Config{
		Bucket:          flagBucket,
		Credentials:     services.NewCredentials(),
		Region:          flagAWSRegion,
		ConfigTbl:       flagDBConfigTable,
		StatsTbl:        flagStatsTable,
		DBtable:         flagTableName,
		Source:          flagSource,
		DBType:          flagDB,
		ProcessedQueue:  flagQueue,
	}

	//configure postgres DB
	dbcred := &config.DBcredentials{
		Database:        flagDBName,
	}
	err := services.GetDBCredentials(cfg, dbcred)
	if err != nil {
		log.Debugf(err.Error())
	}
	if dbcred.Ip == "localhost" {
		dbcred.Ip = ""
	}

	//create serviceContext
	sctx, err := services.NewServiceContext(cfg, dbcred)
	if err != nil {
		log.Debugf(err.Error())
	}
	sctx.DB.Close()

	//configure elasticsearch
	services.SetElasticSearch(flagESHost)
	services.MakeElasticSearchPool(3)

	esClient := services.GetElasticSearchPoolClient()
	defer services.ReturnElasticSearchPoolClient(esClient)

	//configure dynamodb
	dynamoConfig := map[string]string{"region": "us-west-2"}
	aws.SetDynamo(dynamoConfig)

	key := "/Users/ziling/Downloads/unearthed_yids"
	f, err := os.Open(key)
	if err != nil {
		log.Debugf(err.Error())
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	sourceInChan := make(chan string, 19000)

	for scanner.Scan() {
		yid := scanner.Text()
		//log.Infof(yid)
		sourceInChan <- yid
	}

	close(sourceInChan)
	sourceOutChan := make(chan *Pair, 19000)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int, inChan chan string, outChan chan *Pair, wg *sync.WaitGroup) {
			defer wg.Done()
			for yid := range inChan {
				doc, err := getExisting(yid)
				if err != nil {
					log.Debugf(yid)
					log.Debugf(err.Error())
				}

				doc.ContainerId = strings.Replace(doc.ContainerId, "\n", "", -1)
				doc.ContainerId = strings.TrimSpace(doc.ContainerId)

				if doc.ContainerId != "" {
					pair := &Pair{
						Yid: yid,
						ContainerId: doc.ContainerId,
						Chapter: doc.Chapter,
					}
					spew.Dump(doc.Chapter)
					outChan <- pair
				}
			}
		}(i, sourceInChan, sourceOutChan, &wg)
	}
	wg.Wait()
	close(sourceOutChan)

	flagFile := "/Users/ziling/Desktop/yid_containerIds.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for pair := range sourceOutChan {
		line := fmt.Sprintf("%s,%s,%s\n", pair.Yid, pair.ContainerId,pair.Chapter.(string))

		log.Infof(line)

		_, err := file.WriteString(line)
		if err != nil {
			log.WithError(err)
			continue
		}
	}

	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}

}