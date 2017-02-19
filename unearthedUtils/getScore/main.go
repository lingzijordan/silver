package main

import (
	"time"
	"flag"
	"github.com/yewno/log"
	"github.com/yewno/log/handlers/text"
	"github.com/yewno/silver/config"
	"github.com/yewno/silver/services"
	"os"
	"bufio"
	"sync"
	"github.com/davecgh/go-spew/spew"
	"strings"
	"fmt"
	"encoding/json"
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

type YidCid struct {
	YID   string `json:"yid"`
	CID   string `json:"cid"`
	Score string `json:"score"`
}

type Pair struct {
	Yid  string
	Cid  string
}

type Row struct {
	Yid string
	Cid string
	Score string
}

func getScore(yid string, cid string) (string, error) {
	var score string

	dynamo, err := services.GetDynamo()
	if err != nil {
		log.WithError(err).Error("")
		return score, err
	}

	queryExpression := "yid = :yid and cid = :cid"
	var expressionMap = map[string]interface{}{}
	expressionMap[":yid"] = yid
	expressionMap[":cid"] = cid

	fullRecords, err := dynamo.Query("HLDA_UnearthedDocumentConcepts_v2", queryExpression, expressionMap)
	if err != nil {
		log.WithError(err).Error("")
		return score, err
	}

	for _, record := range fullRecords {
		spew.Dump(record)
		bytesArr, err := json.Marshal(record)
		if err != nil {
			log.WithError(err).Error("")
			return score, err
		}

		var doc YidCid

		//log.Infof(string(bytesArr))

		if err := json.Unmarshal(bytesArr, &doc); err != nil {
			spew.Dump(doc)
			log.WithError(err).Error("")
			return score, err
		}

		score = doc.Score
	}

	log.Infof(yid)

	return score, nil
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
	services.SetDynamo(dynamoConfig)

	key := "/Users/ziling/Desktop/yid_cids_books.txt"
	f, err := os.Open(key)
	if err != nil {
		log.Debugf(err.Error())
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	sourceInChan := make(chan *Pair, 13526038)

	var counter int

	for scanner.Scan() {
		line := scanner.Text()
		splitLine := strings.Split(line, ",")
		yid := splitLine[0]
		cids := strings.Split(splitLine[1], " ")

		for _, cid := range cids {
			pair := &Pair{
				Yid: yid,
				Cid: cid,
			}
			counter++
			sourceInChan <- pair
		}
	}

	close(sourceInChan)


	sourceOutChan := make(chan *Row, 13526038)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int, inChan chan *Pair, outChan chan *Row, wg *sync.WaitGroup) {
			defer wg.Done()
			for pair := range inChan {
				score, err := getScore(pair.Yid, pair.Cid)
				if err != nil {
					log.Debugf(err.Error())
				}

				if score != "" {
					row := &Row{
						Yid: pair.Yid,
						Cid: pair.Cid,
						Score: score,
					}

					outChan <- row
				}
			}
		}(i, sourceInChan, sourceOutChan, &wg)
	}
	wg.Wait()
	close(sourceOutChan)

	flagFile := "/Users/ziling/Desktop/yid_cid_score_2.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for row := range sourceOutChan {
		line := fmt.Sprintf("%s,%s,%s\n", row.Yid, row.Cid, row.Score)

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

	println("%d", counter)

}
