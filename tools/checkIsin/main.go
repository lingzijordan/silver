package main

import (
	"os"
	"github.com/yewno/log"
	"github.com/yewno/silver/config"
	"time"
	"flag"
	"github.com/yewno/silver/services"
	"github.com/yewno/log/handlers/text"
	"io/ioutil"
	"encoding/xml"
//	"github.com/yewno/carbon"
	"github.com/yewno/carbon"
	"fmt"
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

type html struct {
	Body []body `xml:"body>p"`
}

type body struct {
	Type string `xml:"class,attr"`
	Value string `xml:",chardata"`
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

	key := "/Users/ziling/Documents/mitpress/9780262322973_4.ypub"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	bytesArr, _ := ioutil.ReadAll(f)
	str, _ := carbon.CleanHTML(string(bytesArr))
	fmt.Println(str)

	var jObj html
	err = xml.Unmarshal([]byte(str), &jObj)
	if err != nil {
		log.Infof(err.Error())
	}

	var chapterTitle string
	for _, p := range jObj.Body {
		if p.Type == "chtitle" || p.Type == "h2"{
			chapterTitle = fmt.Sprintf("%s %s", chapterTitle, p.Value)
		}
	}

	fmt.Println(chapterTitle)

	//spew.Dump(jObj)

}
