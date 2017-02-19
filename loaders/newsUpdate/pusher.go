package newsUpdate

import (
	"strings"
	"path"
	"errors"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func PushToQueue(sctx *services.ServiceContext, pairs []*formats.Pair) error {
	if len(pairs) > 0 {
		var count int
		batch := sctx.Queue.NewBatch(sctx.Cfg.ProcessedQueue)
		for _, p := range pairs {
			if p.Meta != "" && p.Content != "" {
				count++
				yid := strings.Split(path.Base(p.Meta), ".")[0]
				ingestionMsg := &services.PairMessage{
					Source:  "tr-news",
					Bucket:  sctx.Cfg.Bucket,
					Key:     p.Content,
					MetaKey: yid,
				}
				batch.Add(ingestionMsg)
			} else if p.Meta != "" {
				log.WithField("key", p.Meta).Warn("failed to pair")
			} else {
				log.WithField("key", p.Content).Warn("failed to pair")
			}
		}
		batch.Flush()
		log.Infof("added %d pairs", count)

		return nil
	} else {
		return errors.New("doesn't have any paris!!!")
	}
}
