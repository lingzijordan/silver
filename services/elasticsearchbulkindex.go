package services

import (
	"gopkg.in/olivere/elastic.v3"
	"fmt"
	"errors"
	"github.com/davecgh/go-spew/spew"
	//"github.com/Sirupsen/logrus"
	"github.com/yewno/log"
)

type esRequest struct {
	Terms map[string]*elastic.BulkIndexRequest
}

func NewEsRequestMap() *esRequest {
	req := &esRequest{
		Terms: map[string]*elastic.BulkIndexRequest{},
	}

	return req
}

func (ctx *esRequest) Add(index, id string, request *elastic.BulkIndexRequest) {
	ctx.Terms[fmt.Sprintf("%s_%s", index, id)] = request
}

func (ctx *esRequest) Delete(index, id string) {
	delete(ctx.Terms, fmt.Sprintf("%s_%s", index, id))
}

func (ctx *esRequest) Get(index, id string) *elastic.BulkIndexRequest {
	return ctx.Terms[fmt.Sprintf("%s_%s", index, id)]
}

type BulkESIndexer struct {
	RequestMap *esRequest
	Service    *elastic.BulkProcessor
	Client     *elastic.Client
}

func (ctx *BulkESIndexer) AfterBulk(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	// spew.Dump(response)
	if response.Errors {
		err2 := errors.New("failed to upload to elasticsearch")
		spew.Dump(response)
		log.WithError(err)
		log.WithError(err2)
	}
	for _, succeeded := range response.Succeeded() {
		ctx.RequestMap.Delete(succeeded.Index, succeeded.Id)
	}
	failed := response.Failed()
	if len(failed) > 0 {
		for _, failed := range failed {
			ctx.Service.Add(ctx.RequestMap.Get(failed.Index, failed.Id))
		}
		if err := ctx.Service.Flush(); err != nil {
			log.WithError(err)
		}
		//stats := ctx.Service.Stats()
		//log.WithFields(logrus.Fields{"failed": len(failed), "success": len(response.Succeeded()), "stats": stats}).Errorf("elasticsearch index failed")
	}
}

func (ctx *BulkESIndexer) Add(index, docType, docID string, doc interface{}) {
	request := elastic.NewBulkIndexRequest().Index(index).Type(docType).Id(docID).Doc(doc)
	ctx.Service.Add(request)
	//ctx.RequestMap.Add(index, docID, request)
}

func (ctx *BulkESIndexer) Close() {
	ReturnElasticSearchPoolClient(ctx.Client)
}

func (ctx *BulkESIndexer) Stats() elastic.BulkProcessorStats {
	return ctx.Service.Stats()
}

func (ctx *BulkESIndexer) Flush() error {
	return ctx.Service.Flush()
}

func NewBulkESIndexer() (*BulkESIndexer, error) {
	client := GetElasticSearchPoolClient()
	indexer := &BulkESIndexer{
		RequestMap: NewEsRequestMap(),
		Client:     client,
	}

	bulkUploader, err := elastic.NewBulkProcessorService(client).BulkActions(1000).Stats(true).Do() // .After(indexer.AfterBulk)
	indexer.Service = bulkUploader
	return indexer, err
}