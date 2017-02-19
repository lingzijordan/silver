package newsUpdate

import (
	"github.com/yewno/silver/formats"
	"github.com/yewno/carbon/aws"
	"github.com/yewno/log"
)

func UploadMetadata(newsMap map[string]*formats.NewsUpdateMeta) error {
	batch := aws.NewDynamoBatch()
	for _, v := range newsMap {
		log.Debugf("uploading to dynamo table 'contentMetaFinance'")
		if err := batch.Add("contentMetaFinance", *v); err != nil {
			log.WithError(err)
			return err
		}
	}
	if err := batch.Flush(); err != nil {
		log.WithError(err)
		return err
	}
	return nil
}
