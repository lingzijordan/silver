package services

import (
	"fmt"
	"io"
	"io/ioutil"
	//"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/yewno/silver/config"
	"github.com/yewno/log"
	//"path"
)

func NewSilverS3(cfg *config.Config) (*SilverS3, error) {

	conn := s3.New(
		session.New(&aws.Config{
			Region:      aws.String(cfg.Region),
			Credentials: cfg.Credentials,
		}),
	)

	return &SilverS3{
		Conn:     conn,
		Uploader: s3manager.NewUploaderWithClient(conn),
	}, nil
}

type SilverS3 struct {
	Conn     *s3.S3
	Uploader *s3manager.Uploader
}

func (c *SilverS3) Get(object *Object) error {

	params := s3.GetObjectInput{
		Bucket: aws.String(object.Bucket),
		Key:    aws.String(object.Key),
	}

	resp, err := c.Conn.GetObject(&params)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}
	defer resp.Body.Close()

	file, err := ioutil.TempFile("", "")
	if err != nil {
		log.WithError(err).Error("")
		return err
	}

	size, err := io.Copy(file, resp.Body)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}

	object.File = file
	object.Size = size

	_, err = object.File.Seek(0, 0)

	return err
}

func (c *SilverS3) Put(object *Object) error {
	upParams := &s3manager.UploadInput{
		Bucket: aws.String(object.Bucket),
		Key:    aws.String(object.Key),
		Body:   object.File,
	}

	_, err := c.Uploader.Upload(upParams)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}
	return nil
}

func (c *SilverS3) Copy(object *Object, destBucket string, destKey string) error {
	source := fmt.Sprintf("%s/%s", object.Bucket, object.Key)
	log.Infof("Copying: %s > %s/%s", source, destBucket, destKey)
	params := s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		CopySource: aws.String(source),
		Key:        aws.String(destKey),
	}

	_, err := c.Conn.CopyObject(&params)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}
	return err
}

func (c *SilverS3) Save(object *Object) error {
	//var err error
	//switch path.Ext(object.Key) {
	//case ".xml", ".txt", ".json", ".hxml", ".sxml", ".wxml", ".rxml", ".ypub", ".nxml", ".sgm":
	//	err = object.compress()
	//}
	//
	//if err != nil {
	//	log.WithError(err).Error("")
	//	return err
	//}

	//log.Infof("Uploading: %s > %s", object.Key, object.Bucket)
	return c.Put(object)
}
//
//func (c *SilverS3) Stats() *StorageStats {
//	return &StorageStats{}
//}