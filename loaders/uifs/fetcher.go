package uifs

import (
	"fmt"
	"database/sql"
	"strings"
	"github.com/yewno/silver/formats"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/utils"
	"encoding/json"
)

func RequestUrl(code string) string {
	return fmt.Sprintf("https://www.quandl.com/api/v3/datasets/%s.json?api_key=Exa_W1JfKGXQyJw3j2kQ", code)
}

func PullData(db *sql.DB, code *formats.UifsCode, sctx *services.ServiceContext) error {
	url := RequestUrl(code.Code)
	response, err := services.Get(url)
	if err != nil {
		log.WithError(err)
	}
	sctx.Cfg.DBtable = strings.Split(code.Code, "/")[1]
	err = utils.SavetoS3(response, sctx, ".json")
	if err != nil {
		log.WithError(err)
	}
	file := new(formats.UifsJson)
	if err = json.Unmarshal(response, file); err != nil {
		log.WithError(err)
	}

	err = LoadData(db, file, sctx.Cfg.DBtable)

	return nil
}
