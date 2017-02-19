package services

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"sync"
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/aws/aws-sdk-go/aws/session"
	"encoding/json"
	"github.com/yewno/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/yewno/silver/config"
)

type PairMessage struct {
	Source  string `json:"source"`
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
	MetaKey string `json:"metakey,omitempty"`
}

// CobalsSqs is a struct that handles communication for SQS
type SilverSqs struct {
	Conn      *sqs.SQS
	queueURLs map[string]string
}

type SilverSqsBatch struct {
	messages chan *sqs.SendMessageBatchRequestEntry
	conn     *sqs.SQS
	queue    string
	wg       *sync.WaitGroup
}

// NewCobaltSqs will return a queue connection. If key and secret are empty strings then they
// will be pulled from the .aws credentials file.  Queue is the name of the queue and not the
// url. If a queue with that name doesn't exist then an error will be returned.
func NewCobaltSqs(cfg *config.Config, queues ...string) (*SilverSqs, error) {

	conn := sqs.New(session.New(&aws.Config{
		Region:      aws.String(cfg.Region),
		Credentials: cfg.Credentials,
	}))

	queueURLs := make(map[string]string, len(queues))
	for _, q := range queues {
		resp, err := conn.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(q)})
		if err != nil {
			return nil, err
		}
		queueURLs[q] = *resp.QueueUrl

	}

	return &SilverSqs{
		Conn:      conn,
		queueURLs: queueURLs,
	}, nil
}

// NewBatch returns a batch struct that will bundle sqs messages and send them up
// onces the buffer is full (10 messages) or if the last message received was more than
// 10 seconds ago.
func (c *SilverSqs) NewBatch(queue string) *SilverSqsBatch {
	return NewSilverSqsBatch(c.Conn, c.queueURLs[queue])
}

// NewCobaltSqsBatch creates a new batch struct attached to the queue it was called from.
func NewSilverSqsBatch(conn *sqs.SQS, queue string) *SilverSqsBatch {
	var wg sync.WaitGroup

	batch := &SilverSqsBatch{
		messages: make(chan *sqs.SendMessageBatchRequestEntry, 10),
		conn:     conn,
		queue:    queue,
		wg:       &wg,
	}

	wg.Add(1)
	go batch.process()
	return batch
}

// Add adds a message to the batch to be send up to the queue
func (c *SilverSqsBatch) Add(msg interface{}) error {

	b, err := json.Marshal(msg)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}

	entry := &sqs.SendMessageBatchRequestEntry{
		Id:          aws.String(fmt.Sprintf("%s", uuid.NewV4())),
		MessageBody: aws.String(string(b)),
	}

	c.messages <- entry
	return nil
}

// Flush sends any remaining messages to the queue.
func (c *SilverSqsBatch) Flush() {
	close(c.messages)
	c.wg.Wait()
}

func (c *SilverSqsBatch) process() {
	defer c.wg.Done()

	params := &sqs.SendMessageBatchInput{QueueUrl: aws.String(c.queue)}
	var batch []*sqs.SendMessageBatchRequestEntry

	for m := range c.messages {

		batch = append(batch, m)

		if len(batch) < 10 {
			continue
		}

		params.Entries = batch
		batch = c.send(params)

	}

	if len(batch) > 0 {
		params.Entries = batch
		c.send(params)
	}
}

func (c *SilverSqsBatch) send(params *sqs.SendMessageBatchInput) []*sqs.SendMessageBatchRequestEntry {
	var batch []*sqs.SendMessageBatchRequestEntry
	resp, err := c.conn.SendMessageBatch(params)
	if err != nil {
		log.WithError(err).Error("")
		return batch
	}
	if len(resp.Failed) > 0 {
		for _, item := range resp.Failed {
			log.Infof("Code: %s, Message: %s", *item.Code, *item.Message)
		}
	}
	return batch
}