package main

import (
	"os"
	"bufio"
	"fmt"
	"strings"
	"flag"
	"time"
	"github.com/yewno/log"
	"github.com/yewno/silver/config"
	"github.com/yewno/silver/services"
	"github.com/yewno/log/handlers/text"
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

	key := "/Users/ziling/secMeta.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	YidCikMap := make(map[string]string)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")
		yid := arr[0]
		fkey := arr[1]

		fname := strings.Split(fkey, "/")[1]
		cik := strings.Split(fname, "-")[0]

		YidCikMap[yid] = cik

		//fmt.Println("%s : %s", yid, cik)
	}

	key = "/Users/ziling/yid_10KQ.txt"
	f2, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f2.Close()

	yids := []string{}

	scanner2 := bufio.NewScanner(f2)
	for scanner2.Scan() {
		yid := scanner2.Text()
		yids = append(yids, yid)
	}

	cikMap := make(map[string]int)
	for _, yid := range yids {
		cik := YidCikMap[yid]
		cikMap[cik] = 1
	}

	fmt.Println(len(cikMap))

	//yidIssinMap := make(map[string]string)
	//
	//r := []string{}
	//
	//fmt.Println(len(YidCikMap))
	//
	//for yid, cik := range YidCikMap {
	//	issin, ok := cikIssinMap[cik]
	//	if !ok {
	//		issin = "NA"
	//	}
	//
	//	yidIssinMap[yid] = issin
	//	re := fmt.Sprintf("%s : %s\n", yid, issin)
	//	r = append(r, re)
	//}
	//
	//fmt.Println(len(r))
	//
	//flagFile := "/Users/ziling/yidIssinFull.txt"
	//file, err := os.OpenFile(flagFile, os.O_CREATE|os.O_WRONLY, 0644)
	//if err != nil {
	//	log.WithError(err)
	//}
	//defer file.Close()
	//
	//for _, k := range r {
	//	_, err := file.WriteString(k)
	//	if err != nil {
	//		log.WithError(err)
	//		continue
	//	}
	//}
	//
	//if _, err := file.Seek(0, 0); err != nil {
	//	log.WithError(err).Error("")
	//}
	//
	//
	//key = "/Users/ziling/meta_13g.txt"
	//f3, err := os.Open(key)
	//if err != nil {
	//	log.WithError(err)
	//}
	//defer f3.Close()
	//
	//resultArr := []string{}
	//
	//scanner3 := bufio.NewScanner(f3)
	//for scanner3.Scan() {
	//	line := scanner3.Text()
	//
	//	arr := strings.Split(line, ",")
	//	year := arr[0]
	//	month := arr[1]
	//	day := arr[2]
	//	yid := arr[3]
	//
	//	issin, ok := yidIssinMap[yid]
	//	if !ok {
	//		issin = "NA"
	//	}
	//
	//	str := fmt.Sprintf("%s,%s,%s,%s,%s\n", year, month, day, yid, issin)
	//	resultArr = append(resultArr, str)
	//}
	//
	//flagFile = "/Users/ziling/meta_13g_date.txt"
	//file, err = os.OpenFile(flagFile, os.O_CREATE|os.O_WRONLY, 0644)
	//if err != nil {
	//	log.WithError(err)
	//}
	//defer file.Close()
	//
	//for _, k := range resultArr {
	//	_, err := file.WriteString(k)
	//	if err != nil {
	//		log.WithError(err)
	//		continue
	//	}
	//}
	//
	//if _, err := file.Seek(0, 0); err != nil {
	//	log.WithError(err).Error("")
	//}
}
