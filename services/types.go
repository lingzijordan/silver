package services

import (
	"github.com/yewno/cobalt-50/stat"
	"sync"
	"github.com/yewno/silver/config"
	"database/sql"
	"github.com/yewno/log"
)

type ServiceContext struct {
	DB      *sql.DB
	Storage *SilverS3
	Queue   *SilverSqs
	Cfg     *config.Config
	Stats   *stat.Stats
	DBcred  *config.DBcredentials

	wg *sync.WaitGroup
}

func NewServiceContext(cfg *config.Config, dbcred *config.DBcredentials) (*ServiceContext, error) {
	var wg sync.WaitGroup
	var ctx = &ServiceContext{Cfg: cfg, DBcred:dbcred, wg: &wg}
	storage, err := NewSilverS3(cfg)
	if err != nil {
		return ctx, err
	}
	ctx.Storage = storage
	queue, err := NewCobaltSqs(cfg, cfg.ProcessedQueue)
	if err != nil {
		return ctx, err
	}
	ctx.Queue = queue

	db, err := ConnectDB(ctx.DBcred, cfg.DBType)
	if err != nil {
		log.WithError(err)
	}
	ctx.DB = db

	return ctx, nil
}

func NewServiceContextWithoutDB(cfg *config.Config) (*ServiceContext, error) {
	var wg sync.WaitGroup
	var ctx = &ServiceContext{Cfg: cfg, wg: &wg}
	storage, err := NewSilverS3(cfg)
	if err != nil {
		return ctx, err
	}
	ctx.Storage = storage
	queue, err := NewCobaltSqs(cfg, cfg.ProcessedQueue)
	if err != nil {
		return ctx, err
	}
	ctx.Queue = queue

	return ctx, nil
}
