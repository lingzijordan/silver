package services

import (
	"os"
	//"io/ioutil"
	//"compress/gzip"
	//"io"
	//"fmt"
	"github.com/yewno/log"
)

type Object struct {
	File   *os.File
	Bucket string
	Key    string
	Size   int64
}

func NewObject(file *os.File, bucket, key string, size int64) *Object {
	return &Object{
		File:   file,
		Bucket: bucket,
		Key:    key,
		Size:   size,
	}
}

func (o *Object) Close() error {
	if o.File != nil {
		if err := o.File.Close(); err != nil {
			log.WithError(err).Error("")
			return err
		}

		if err := os.Remove(o.File.Name()); err != nil {
			log.WithError(err).Error("")
			return err
		}
	}
	return nil
}

//func (o *Object) compress() error {
//
//	temp, err := ioutil.TempFile("", "")
//	if err != nil {
//		log.WithError(err).Error("")
//		return err
//	}
//
//	writer := gzip.NewWriter(temp)
//
//	if _, err = io.Copy(writer, o.File); err != nil {
//		log.WithError(err).Error("")
//		return err
//	}
//
//	o.File = temp
//	o.Key = fmt.Sprintf("%s.gz", o.Key)
//
//	if err = writer.Close(); err != nil {
//		log.WithError(err).Error("")
//		return err
//	}
//
//	_, err = temp.Seek(0, 0)
//	return err
//}
