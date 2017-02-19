package secGovMeta

import (
	"errors"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
)

func PushToQueue(sctx *services.ServiceContext, msgMap map[string]string) error {
	if len(msgMap) > 0 {
		var count int
		batch := sctx.Queue.NewBatch(sctx.Cfg.ProcessedQueue)
		for yid, loc := range msgMap {
			count++
			ingestionMsg := &services.PairMessage{
				Source:  "sec-gov",
				Bucket:  sctx.Cfg.Bucket,
				Key:     loc,
				MetaKey: yid,
			}
			batch.Add(ingestionMsg)
		}
		batch.Flush()
		log.Infof("added %d msgs", count)

		return nil
	} else {
		return errors.New("doesn't have any msgs!!!")
	}
}
