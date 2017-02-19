package services

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/davecgh/go-spew/spew"
	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/yewno/log"
)

var (
	dynamoClient *Dynamo
	dynamoConfig *aws.Config
)

func SetDynamo(config map[string]string) {
	if dynamoConfig == nil {
		creds := credentials.NewChainCredentials(
			[]credentials.Provider{
				&ec2rolecreds.EC2RoleProvider{
					Client: ec2metadata.New(session.New()),
				},
				&credentials.SharedCredentialsProvider{},
				//&credentials.EnvProvider{},
			})
		_, err := creds.Get()
		if err != nil {
			log.Error("unable to retrieve credentials")
			panic(err)
		}
		newConfig := aws.NewConfig().WithCredentials(creds)
		for k, v := range config {
			switch k {
			case "endpoint":
				newConfig = newConfig.WithEndpoint(v)
			case "region":
				newConfig = newConfig.WithRegion(v)
			}
		}
		log.WithField("config", config).Debug("setting dynamo config")
		dynamoConfig = newConfig
	} else {
		log.Warn("config already set")
	}
}

func GetDynamo() (*Dynamo, error) {
	if dynamoClient == nil {
		log.Debug("no dynamo client, creating")
		dynamoClient = &Dynamo{
			Client: NewDynamo(dynamoConfig),
		}
	}
	return dynamoClient, nil
}

func NewDynamoDB() *Dynamo {
	return &Dynamo{
		Client: dynamodb.New(session.New(), dynamoConfig),
	}
}

func NewDynamo(config *aws.Config) *dynamodb.DynamoDB {
	s := session.New()
	dynamo := dynamodb.New(s, config)
	log.Debug("new dynamo")
	return dynamo
}

type Dynamo struct {
	Client *dynamodb.DynamoDB
}

func (t *Dynamo) CreateTable() error {

	return nil
}

func (t *Dynamo) ListTables() ([]string, error) {
	var lastTableName *string
	tables := []string{}

	for {
		params := &dynamodb.ListTablesInput{
			ExclusiveStartTableName: lastTableName,
		}
		resp, err := t.Client.ListTables(params)
		if err != nil {
			return nil, err
		}
		for _, tbl := range resp.TableNames {
			tables = append(tables, *tbl)
		}
		if resp.LastEvaluatedTableName != nil {
			lastTableName = resp.LastEvaluatedTableName
		} else {
			break
		}
	}
	return tables, nil
}

func (t *Dynamo) GetRecord(tablename, key, value string) (bool, map[string]interface{}, error) {
	results := map[string]interface{}{}

	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			key: {S: aws.String(value)},
		},
		TableName: aws.String(tablename),
	}
	resp, err := t.Client.GetItem(params)
	if err != nil {
		// TODO : check if this is actually real - appears to just give back
		// an empty resp.Item
		if awsErr, ok := err.(awserr.RequestFailure); ok {
			if awsErr.StatusCode() == 404 {
				log.Infof("record not found", value)
				return false, results, nil
			}
		}
		return false, results, err
	}
	if resp.Item != nil {
		if err := dynamodbattribute.ConvertFromMap(resp.Item, &results); err != nil {
			log.WithError(err)
			return false, results, err
		}
		return true, results, nil
	}
	return false, results, nil
}

func (t *Dynamo) UpdateRecord(tablename, key, value, updateExpression string, values map[string]interface{}, expressionMap map[string]*string) error {

	updateValues, err := dynamodbattribute.ConvertToMap(values)
	if err != nil {
		log.WithError(err)
		return err
	}

	params := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			key: {S: aws.String(value)},
		},
		TableName:                 aws.String(tablename),
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: updateValues,
		ExpressionAttributeNames:  expressionMap,
	}
	_, err = t.Client.UpdateItem(params)
	if err != nil {
		log.WithError(err)
		return err
	}
	return nil
}

func (t *Dynamo) Query(tablename, queryExpression string, values map[string]interface{}) ([]map[string]interface{}, error) {
	var output []map[string]interface{}

	queryValues, err := dynamodbattribute.ConvertToMap(values)
	if err != nil {
		log.WithError(err)
		return output, err
	}

	params := &dynamodb.QueryInput{
		TableName:                 aws.String(tablename),
		KeyConditionExpression:    aws.String(queryExpression),
		ExpressionAttributeValues: queryValues,
	}
	resp, err := t.Client.Query(params)
	if err != nil {
		log.WithError(err)
		return output, err
	}
	log.Debugf("query returned %d items", *resp.Count)
	for _, item := range resp.Items {
		var out map[string]interface{}
		if err := dynamodbattribute.UnmarshalMap(item, &out); err != nil {
			log.WithError(err)
			spew.Dump(resp)
			return output, err
		}
		output = append(output, out)
	}

	return output, nil
}

func (t *Dynamo) PutRecord(tablename string, record interface{}) error {
	item, err := dynamodbattribute.ConvertToMap(record)
	if err != nil {
		log.WithError(err)
		spew.Dump(record)
		return err
	}

	params := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(tablename),
	}
	_, err = t.Client.PutItem(params)
	if err != nil {
		log.WithError(err)
		return err
	}
	return nil
}