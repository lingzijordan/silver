package federalReserve

import (
	"fmt"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/utils"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	"encoding/json"
)

func RequestUrl(code string) string {
	return fmt.Sprintf("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=aa768fa6b2d10953ddae7065782af546&file_type=json", code)
}

func PullData(sctx *services.ServiceContext, code string) error {

	url := RequestUrl(code)
	//url2 := "https://api.stlouisfed.org/fred/series/observations?series_id=RPI&api_key=aa768fa6b2d10953ddae7065782af546&file_type=json"
	response, err := services.Get(url)

	sctx.Cfg.DBtable = code

	err = utils.SavetoS3(response, sctx, ".json")

	log.Infof("%s", url)

	if err != nil {
		log.WithError(err)
	}

	file := new(formats.FRJson)
	if err = json.Unmarshal(response, file); err != nil {
		log.WithError(err)
		return err
	}

	//spew.Dump(file)

	err = LoadData(sctx.DB, file, code)

	return err
}
