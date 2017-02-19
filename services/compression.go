package services

import (
	"github.com/yewno/carbon"
	"os"
	"io/ioutil"
	"compress/bzip2"
	"errors"
	"github.com/yewno/log"
	"compress/gzip"
)

func DetectCompression(filename string) string {
	var ext string
	extension, err := carbon.DetectByExtention(filename)
	if err != nil {
		log.WithError(err)
	}
	switch extension {
	case ".gz", ".gzip":
		return "gzip"
	case ".bz2", ".bzip2":
		return "bzip2"
	}
	log.Debugf(filename, extension)
	return ext
}

// TODO tar.gz .zip
func Decompress(filename, compression string) ([]byte, error) {
	var data []byte
	f, err := os.Open(filename)
	if err != nil {
		log.WithError(err)
		return nil, err
	}
	defer f.Close()

	switch compression {
	case "gzip":
		reader, err := gzip.NewReader(f)
		if err != nil {
			log.WithError(err)
			return nil, err
		}
		if data, err = ioutil.ReadAll(reader); err != nil {
			log.WithError(err)
			return nil, err
		}
	case "bzip2":
		reader := bzip2.NewReader(f)
		if data, err = ioutil.ReadAll(reader); err != nil {
			log.WithError(err)
			return nil, err
		}
	default:
		return nil, errors.New("file does not appear to be compressed")
	}

	return data, nil
}