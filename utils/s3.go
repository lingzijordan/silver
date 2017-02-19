package utils

import (
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"fmt"
	"time"
	"strings"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

func SavetoS3WithFileName(response []byte, sctx *services.ServiceContext, fname, ext string) error{
	file, size, err := BytesToFile(response)
	if err != nil {
		log.WithError(err)
	}

	fname = fmt.Sprintf("%s%s", fname, ext)
	key := fmt.Sprintf("%s/%s/%s", sctx.Cfg.Source, time.Now().Format("20060102"), fname)

	obj := services.NewObject(file, sctx.Cfg.Bucket, key, size)
	err = sctx.Storage.Save(obj)
	if err != nil {
		log.WithError(err)
	}

	return err
}

func SavetoS3(response []byte, sctx *services.ServiceContext, ext string) error{
	file, size, err := BytesToFile(response)
	if err != nil {
		log.WithError(err)
	}

	fname := fmt.Sprintf("%s%s", sctx.Cfg.DBtable, ext)
	key := fmt.Sprintf("%s/%s/%s", sctx.Cfg.Source, time.Now().Format("20060102"), fname)

	obj := services.NewObject(file, sctx.Cfg.Bucket, key, size)
	err = sctx.Storage.Save(obj)
	if err != nil {
		log.WithError(err)
	}

	return err
}

func SavetoS3CustomKey(response []byte, bucket, key string, sctx *services.ServiceContext) error{
	file, size, err := BytesToFile(response)
	if err != nil {
		log.WithError(err)
	}

	obj := services.NewObject(file, bucket, key, size)
	err = sctx.Storage.Save(obj)
	if err != nil {
		log.WithError(err)
	}

	return err
}

func S3ListObjectsWithTimestamp(conn *s3.S3, bucket string, prefix ...string) (map[string]time.Time, error) {
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}
	if len(prefix) > 0 {
		prefixString := prefix[0]
		strings.TrimSuffix(prefixString, "*")
		params.Prefix = aws.String(prefixString)
	}
	pageNum := 0
	keys := map[string]time.Time{}
	conn.ListObjectsPages(params, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		pageNum++
		for _, value := range page.Contents {
			if strings.HasSuffix(*value.Key, "/") {
				log.Debugf("directory: %s", *value.Key)
				continue
			}
			//keys = append(keys, *value.Key)
			keys[*value.Key] = *value.LastModified
		}
		log.Debugf("page: %d", pageNum)
		return len(page.Contents) == 1000
	})
	log.WithField("total", len(keys)).Info("keys retrieved")
	return keys, nil
}

func FilterDate(keys map[string]time.Time, date time.Time) []string {
	var keysFiltered = []string{}
	for key, keyDate := range keys {
		if (keyDate.After(date)) {
			keysFiltered = append(keysFiltered, key)
		}
	}
	return keysFiltered
}

func RetrieveKeys(sctx *services.ServiceContext, flagTimeFrame time.Duration) []string {
	//flagTimeFrame := 30 * 24 * time.Hour
	startDate := time.Now().Add(-flagTimeFrame)
	keys, err := S3ListObjectsWithTimestamp(sctx.Storage.Conn, sctx.Cfg.Bucket, sctx.Cfg.Source)
	if err != nil {
		log.WithError(err).Fatal("")
	}
	keysFilteredByDate := FilterDate(keys, startDate)
	log.Infof("keysFilteredByDate count %s", len(keysFilteredByDate))

	return keysFilteredByDate
}
