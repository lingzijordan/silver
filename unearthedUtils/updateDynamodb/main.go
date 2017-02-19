package main

import (
	"os"
	"bufio"
	"time"
	"flag"
	"github.com/yewno/log"
	"github.com/yewno/log/handlers/text"
	"github.com/yewno/silver/config"
	"github.com/yewno/silver/services"
	"strings"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/davecgh/go-spew/spew"
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
	flagQueue   string
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

	log.SetHandler(text.Default)
	flag.Parse()
}

type Pair struct {
	Yid string
	ChapterTitle string
}

func main() {
	log.SetLevel(log.DebugLevel)

	dynamoConfig := map[string]string{"region": "us-west-2"}
	services.SetDynamo(dynamoConfig)

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

	sctx, err := services.NewServiceContext(cfg, dbcred)
	if err != nil {
		log.Debugf(err.Error())
	}
	sctx.DB.Close()

	key := "/Users/ziling/yids_chapterTitle.txt"
	f, err := os.Open(key)
	if err != nil {
		log.Debugf(err.Error())
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	sourceInChan := make(chan *Pair, 18000)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		pair := &Pair{
			Yid: arr[0],
			ChapterTitle: arr[1],
		}
		sourceInChan <- pair
	}

	close(sourceInChan)
	dynamo, err := services.GetDynamo()
	if err != nil {
		log.WithError(err).Error("")
	}

	table := "contentMeta"
	req := &dynamodb.DescribeTableInput{
		TableName: &table,
	}
	resp, err := dynamo.Client.DescribeTable(req)
	if err != nil {
		log.Debugf(err.Error())
	}
	spew.Dump(resp)

	//for pair := range sourceInChan {
	//	expression := "SET chapterTitle = :chapterTitle"
	//	var expressionMap = map[string]interface{}{}
	//	expressionMap[":chapterTitle"] = pair.ChapterTitle
	//	if err := dynamo.UpdateRecord("contentMeta", "yId", pair.Yid, expression, expressionMap, nil); err != nil {
	//		log.WithError(err).Error("")
	//	}
	//	log.Infof("updated %s : %s", pair.Yid, pair.ChapterTitle)
	//}
}
