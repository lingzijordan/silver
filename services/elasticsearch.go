package services

import (
	"gopkg.in/olivere/elastic.v3"
	"github.com/yewno/log"
)

var (
	es     *elastic.Client
	esHost elastic.ClientOptionFunc
	pool *ElasticSearchPool
)

type ElasticSearchPool struct {
	C chan *elastic.Client
}

func SetElasticSearch(host string) {
	log.Infof("es host:", host)
	esHost = elastic.SetURL(host)
}

func GetElasticSearchClient() *elastic.Client {
	esClient, err := elastic.NewClient(elastic.SetSniff(false), esHost)
	if err != nil {
		log.WithError(err)
		panic(err)
	}
	return esClient
}

func ESDocumentExists(index, docType, docId string) (bool, error) {
	esClient := GetElasticSearchClient()
	return elastic.NewExistsService(esClient).Index(index).Type(docType).Id(docId).Do()
}



func (ctx *ElasticSearchPool) Return(c *elastic.Client) {
	ctx.C <- c
}

func (ctx *ElasticSearchPool) GetClient() *elastic.Client {
	return <-ctx.C
}
func MakeElasticSearchPool(size int) {
	pool = &ElasticSearchPool{
		C: make(chan *elastic.Client, size),
	}
	for i := 0; i < size; i++ {
		pool.C <- GetElasticSearchClient()
	}
}

func GetElasticSearchPoolClient() *elastic.Client {
	return pool.GetClient()
}

func ReturnElasticSearchPoolClient(c *elastic.Client) {
	pool.C <- c
}