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
	"github.com/yewno/carbon/aws"
	"github.com/davecgh/go-spew/spew"
	"encoding/json"
	"compress/gzip"
	"io/ioutil"
	"github.com/yewno/carbon"
	"encoding/xml"
	"fmt"
	"sync"
	"path"
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

type MinimalDoc struct {
	YID             string `json:"yId"`
	S3File          string `json:"s3File"`
	Type            string `json:"type"`
}

func getExisting(yid string) (*MinimalDoc, error) {
	var doc MinimalDoc
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

type Pair struct {
	Yid string
	ChapterTitle string
}

func captureChapterTitle(sctx *services.ServiceContext, key string) (string, error) {
	object := services.NewObject(nil, "yewno-content", key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return "", err
	}
	defer object.Close()

	reader, err := gzip.NewReader(object.File)
	if err != nil {
		log.WithError(err).Error("")
		return "", err
	}
	bytesArr, _ := ioutil.ReadAll(reader)
	str, _ := carbon.CleanHTML(string(bytesArr))
	//fmt.Println(str)

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

	return chapterTitle, nil
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

	key := "/Users/ziling/Desktop/yids.txt"
	f, err := os.Open(key)
	if err != nil {
		log.Debugf(err.Error())
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	sourceInChan := make(chan string, 18000)

	for scanner.Scan() {
		yid := scanner.Text()
		log.Infof(yid)
		sourceInChan <- yid
	}

	close(sourceInChan)
	sourceOutChan := make(chan *Pair, 18000)
	pdfChan := make(chan string, 18000)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int, inChan, pdf chan string, outChan chan *Pair, wg *sync.WaitGroup) {
			defer wg.Done()
			for yid := range inChan {
				doc, err := getExisting(yid)
				if err != nil {
					log.Debugf(yid)
					log.Debugf(err.Error())
				}
				ext := path.Ext(doc.S3File)
				if ext != ".gz" {continue}
				spew.Dump(doc)

				if doc.Type == "book" || ext == ".pdf" {
					pdf <- yid
					continue
				}

				if doc.Type == "chapter" {
					chapterTitle, err := captureChapterTitle(sctx, doc.S3File)
					if err != nil {
						log.Debugf(yid)
						log.Debugf(err.Error())
						continue
					}
					if strings.TrimSpace(chapterTitle) == "" {
						continue
					}

					pair := &Pair{
						Yid: yid,
						ChapterTitle: chapterTitle,
					}

					log.Infof("%s : %s", yid, chapterTitle)

					outChan <- pair

				} else {
					//log.Infof("yid %s is not a chapter!", yid)
					continue
				}
			}
		}(i, sourceInChan, pdfChan, sourceOutChan, &wg)
	}
	wg.Wait()
	close(sourceOutChan)
	close(pdfChan)

	flagFile := "/Users/ziling/yids_chapterTitle.txt"
	file, err := os.OpenFile(flagFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for pair := range sourceOutChan {
		pair.ChapterTitle = strings.TrimSpace(pair.ChapterTitle)
		line := fmt.Sprintf("%s,%s\n", pair.Yid, pair.ChapterTitle)
		_, err := file.WriteString(line)
		if err != nil {
			log.WithError(err)
			continue
		}
	}

	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}

	var counter int
	for _ = range pdfChan {
		counter++
	}

	fmt.Println("%d", counter)
}
