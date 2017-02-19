package deadTickers

import (
	"fmt"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/utils"
	"github.com/yewno/log"
	"bytes"
)

func RequestUrl(ticker string) string {
	return fmt.Sprintf("http://chart.finance.yahoo.com/table.csv?s=%s&a=1&b=1&c=2000&d=11&e=17&f=2016&g=d&ignore=.csv", ticker)
}

func PullData(sctx *services.ServiceContext, ticker string) (bool, error) {

	url := RequestUrl(ticker)
	response, err := services.Get(url)
	if err != nil {
		log.WithError(err)
		return false, err
	}

	if bytes.Contains(response, []byte("Date")) {
		err = utils.SavetoS3WithFileName(response, sctx, ticker, ".csv")
		if err != nil {
			log.WithError(err)
			return false, err
		}
		return true, nil
	}

	return false, nil
}