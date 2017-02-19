package main

import (
	"flag"
	"github.com/yewno/log"
	"github.com/yewno/log/handlers/text"
	"os"
	"bufio"
	"strings"
	"github.com/davecgh/go-spew/spew"
	"gopkg.in/olivere/elastic.v3"
	"io"
	"fmt"
)

var flagESHost string

func init() {
	flag.StringVar(&flagESHost, "eshost", "http://elasticsearch.yewno.io:9200/", "elastic search host address")

	log.SetHandler(text.Default)
	flag.Parse()
}

type Pair struct {
	Yid string
	Cid string
}

func main() {
	//configure elasticsearch
	esClient, err := elastic.NewClient(elastic.SetURL(flagESHost), elastic.SetSniff(false))
	if err != nil {
		log.WithError(err).Fatal("")
	}

	var bulkService *elastic.BulkProcessor

	bulkService, err = elastic.NewBulkProcessorService(esClient).BulkActions(500).Stats(true).Do()
	if err != nil {
		log.WithError(err).Fatal("unable to create bulk processor")
	}

	spew.Dump(bulkService)

	key := "/Users/ziling/Desktop/yid_cids.txt"
	f, err := os.Open(key)
	if err != nil {
		log.Debugf(err.Error())
	}
	defer f.Close()
	reader := bufio.NewReader(f)

	sourceInChan := make(chan *Pair, 218137)

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}

		log.Infof(line)

		arr := strings.Split(line, ",")

		if len(arr) < 2 {
			spew.Dump(arr)
		}

		cid := arr[1]
		cid = strings.Replace(cid, "\n", "", -1)

		pair := &Pair{
			Yid: arr[0],
			Cid: cid,
		}

		sourceInChan <- pair
	}

	close(sourceInChan)

	sourceOutChan := make(chan string, 218137)

	var counter int

	for pair := range sourceInChan {
		yid := pair.Yid
		cid := pair.Cid

		//request := elastic.NewBulkIndexRequest().Index("unearthed_master").Type("contentMeta").Id(yid).Doc(map[string]interface{}{"concepts": cid})
		//bulkService.Add(request)
		_, err := elastic.NewUpdateService(esClient).
			Index("unearthed_master_20170117").
			Type("contentMeta").
			Id(yid).
			Doc(map[string]interface{}{"concepts": cid}).
			Do()
		if err != nil {
			//log.Infof(err.Error())
			sourceOutChan <- yid
			counter++
			continue
		}

		log.Infof(yid)
	}
	close(sourceOutChan)

	if err = bulkService.Flush(); err != nil {
		log.WithError(err).Error("bulk flush 2")
	}

	log.Infof("%d", counter)


	flagFile := "/Users/ziling/Desktop/missing_yids.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for yid := range sourceOutChan {
		line := fmt.Sprintf("%s\n", yid)
		_, err := file.WriteString(line)
		if err != nil {
			log.WithError(err)
			continue
		}
	}

	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}

	//Script(elastic.NewScript("ctx._source.new_field = \"concepts\"").Param("concepts", cid)).

}
