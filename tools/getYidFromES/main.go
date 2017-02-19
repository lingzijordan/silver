package main

import (
	"flag"
	"github.com/yewno/log"
	"github.com/yewno/log/handlers/text"
	"gopkg.in/olivere/elastic.v3"
	"github.com/davecgh/go-spew/spew"
	"os"
	"fmt"
)

var flagESHost string

func init() {
	flag.StringVar(&flagESHost, "eshost", "http://elasticsearch.yewno.io:9200/", "elastic search host address")

	log.SetHandler(text.Default)
	flag.Parse()
}

func main() {

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

	sourceMatch := elastic.NewMatchQuery("source", "mit-collections")
	esquery := elastic.NewBoolQuery().Must(sourceMatch)

	results, err := elastic.NewSearchService(esClient).Index("content_meta").Type("contentmeta").MinScore(2.0).Query(esquery).Field("source").Size(50000).Do()
	if err != nil {
		log.Error(err.Error())
		return
	}

	var yids []string

	if results.TotalHits() > 0 {
		for _, hit := range results.Hits.Hits {
			yids = append(yids, hit.Id)
		}
	}

	flagFile := "/Users/ziling/Desktop/yid_mit-collections_2.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for _, yid := range yids {
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

}
