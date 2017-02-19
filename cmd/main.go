package main

import (
	"github.com/yewno/log"
	"github.com/yewno/silver/config"
	"time"
	"flag"
	"github.com/yewno/log/handlers/text"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/loaders"
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

func main() {

	c := map[string]string{"region": "us-west-2"}
	aws.SetDynamo(c)

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
	defer sctx.DB.Close()

	lFn := loaders.LoadProcessor(flagSource)
	err = loaders.Load(flagSource, lFn, sctx, flagTimeFrame)
	if err != nil {
		log.Fatal(err.Error())
	}
}
