package sec

import (
	"fmt"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/utils"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	"encoding/xml"
	"golang.org/x/net/html/charset"
	"bytes"
)

func RequestUrl(ticker string) string {
	return fmt.Sprintf("https://www.sec.gov/cgi-bin/browse-edgar?CIK=%s&Find=Search&owner=exclude&action=getcompany&output=atom", ticker)
}

func GetCik(sctx *services.ServiceContext, ticker string) (string, error) {

	url := RequestUrl(ticker)
	//url2 := "https://api.stlouisfed.org/fred/series/observations?series_id=RPI&api_key=aa768fa6b2d10953ddae7065782af546&file_type=json"
	response, err := services.Get(url)
	if err != nil {
		log.WithError(err)
		return "", err
	}
	sctx.Cfg.DBtable = ticker

	err = utils.SavetoS3(response, sctx, ".xml")

	log.Infof("%s", url)

	if err != nil {
		log.WithError(err)
		return "", err
	}

	cik := new(formats.CikXml)
	reader := bytes.NewReader(response)
	decoder := xml.NewDecoder(reader)
	decoder.CharsetReader = charset.NewReaderLabel
	err = decoder.Decode(&cik)
	if err != nil {
		log.Infof("decoder error:", err)
	}
	//if err = xml.Unmarshal(response, cik); err != nil {
	//	log.WithError(err)
	//	return "", err
	//}

	//spew.Dump(file)

	log.Infof("cik for ticker %s is %s", ticker, cik.Cik)

	return cik.Cik, err
}
