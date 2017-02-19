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
	"github.com/yewno/carbon/aws"
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

type Row struct {
	Yid string  `json:"yid"`
	Cid string  `json:"cid"`
    Score string `json:"score"`
}

func UploadMetadata(rows chan *Row) error {
	batch := aws.NewDynamoBatch()
	for v := range rows {
		log.Debugf("uploading to dynamo table 'HLDA_UnearthedDocumentConcepts_v3'")
		if err := batch.Add("HLDA_UnearthedDocumentConcepts_v3", *v); err != nil {
			log.WithError(err)
			return err
		}
	}
	if err := batch.Flush(); err != nil {
		log.WithError(err)
		return err
	}
	return nil
}

func main() {
	log.SetLevel(log.DebugLevel)

	dynamoConfig := map[string]string{"region": "us-west-2"}
	aws.SetDynamo(dynamoConfig)

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

	key := "/Users/ziling/Desktop/yid_cid_score_2.txt"
	f, err := os.Open(key)
	if err != nil {
		log.Debugf(err.Error())
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	sourceInChan := make(chan *Row, 9333549)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		cid := arr[1]
		score := arr[2]

		row := &Row{
			Yid: yid,
            Cid: cid,
			Score: score,
 		}
		sourceInChan <- row
	}
	close(sourceInChan)

	err = UploadMetadata(sourceInChan)
	if err != nil {
		log.WithError(err)
	}
}
