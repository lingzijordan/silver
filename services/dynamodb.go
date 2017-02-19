package services

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/yewno/silver/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/yewno/log"
)

// Message
type DbRecord struct {
	Table   string
	Key     string
	SortKey string
	Record  interface{}
}

type SilverDynamo struct {
	cfg  *config.Config
	Conn *dynamodb.DynamoDB
}

func NewSilverDynamo(cfg *config.Config) (*SilverDynamo, error) {
	conn := dynamodb.New(
		session.New(&aws.Config{
			Region:      aws.String(cfg.Region),
			Credentials: cfg.Credentials,
		}),
	)

	return &SilverDynamo{
		cfg:  cfg,
		Conn: conn,
	}, nil
}

func (ctx *SilverDynamo) Get(tablename string, keymap map[string]string, result *map[string]interface{}) (bool, error) {
	params := &dynamodb.GetItemInput{
		Key:       map[string]*dynamodb.AttributeValue{},
		TableName: aws.String(tablename),
	}
	for k, v := range keymap {
		params.Key[k] = &dynamodb.AttributeValue{S: aws.String(v)}
	}

	resp, err := ctx.Conn.GetItem(params)
	if err != nil {
		// TODO : check if this is actually real - appears to just give back
		// an empty resp.Item
		if awsErr, ok := err.(awserr.RequestFailure); ok {
			if awsErr.StatusCode() == 404 {
				log.Infof("record not found %v", keymap)
				return false, nil
			}
		}
		return false, err
	}
	if resp.Item != nil {
		if err := dynamodbattribute.ConvertFromMap(resp.Item, result); err != nil {
			log.WithError(err).Error("")
			return false, err
		}
	}
	return true, nil
}

func (ctx *SilverDynamo) GetAll(key string) ([]DbRecord, error) {
	var records []DbRecord
	return records, nil
}

func (ctx *SilverDynamo) Put(table string, record interface{}) error {
	item, err := dynamodbattribute.ConvertToMap(record)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}

	params := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(table),
	}
	if _, err := ctx.Conn.PutItem(params); err != nil {
		log.WithError(err).Error("")
		return err
	}
	return nil
}
