package news

import (
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"compress/gzip"
	"encoding/csv"
	"github.com/yewno/silver/utils"
	"github.com/yewno/silver/formats"
	"io"
	"io/ioutil"
	"os"
)


func Process(sctx *services.ServiceContext, key string) error {
	tmpdir, err := ioutil.TempDir("/tmp", sctx.Cfg.Source)
	if err != nil {
		log.WithError(err).Error("unable to make tmp dir")
	}
	defer os.RemoveAll(tmpdir)

	newsMap := make(map[string]*formats.NewsContentMeta)

	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return err
	}
	defer object.Close()

	reader, err := gzip.NewReader(object.File)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}
	r := csv.NewReader(reader)
	r.Comma = ','

	for {

		var newsEntry formats.NewsContent
		err := utils.CsvUnmarshal(r, &newsEntry)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.WithError(err)
			return err
		}
		if newsEntry.LANGUAGE != "EN" {
			continue
		}
		if newsEntry.EVENT_TYPE == "DELETE" {
			continue
		}

		meta := ConvertNewsEntry(&newsEntry, sctx)

		if FilterOnTopics(meta.Topics) {
			continue
		}

		newsMeta, ok := newsMap[newsEntry.PNAC]
		if !ok {
			newsMap[newsEntry.PNAC] = meta
		} else {
			newsMap[newsEntry.PNAC] = MergeEntries(newsMeta, meta)
		}
	}

	//cache data to s3
	pairs, err := CacheNewsToS3(sctx, newsMap, tmpdir)
	if err != nil {
		log.WithError(err)
		return err
	}

	//insert metadata to dynamoDB
	err = UploadMetadata(newsMap)
	if err != nil {
		log.WithError(err)
		return err
	}

	//sending sqs messages now
	err = PushToQueue(sctx, pairs)
	if err != nil {
		log.WithError(err)
		return err
	}

	return nil
}
