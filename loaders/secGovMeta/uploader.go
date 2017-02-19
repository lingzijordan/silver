package secGovMeta

import (
	"github.com/yewno/silver/formats"
	"github.com/yewno/carbon/aws"
	"github.com/yewno/log"
)

func UploadMetadata(secMeta map[string]*formats.SecContentMeta) error {
	batch := aws.NewDynamoBatch()
	for _, v := range secMeta {
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
