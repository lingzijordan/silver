package taxonomyIndustry

import (
	"github.com/yewno/silver/services"
	"io/ioutil"
	"os"
	"github.com/yewno/log"
	"compress/gzip"
	"github.com/yewno/silver/formats"
	"encoding/json"
)

func Process(sctx *services.ServiceContext, key string) error {

	tmpdir, err := ioutil.TempDir("/tmp", sctx.Cfg.Source)
	if err != nil {
		log.WithError(err).Error("unable to make tmp dir")
		return err
	}
	defer os.RemoveAll(tmpdir)

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

	bytesArr, err := ioutil.ReadAll(reader)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}
	reader.Close()

	file := new(formats.Industries)
	if err = json.Unmarshal(bytesArr, file); err != nil {
		log.WithError(err)
		return err
	}

	err = LoadData(sctx.DB, file, sctx.Cfg.DBtable)

	return err
}

