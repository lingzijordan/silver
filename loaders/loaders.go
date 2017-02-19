package loaders

import (
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/loaders/companyCustomer"
	"github.com/yewno/silver/loaders/companySegment"
	"github.com/yewno/silver/loaders/companyShareholder"
	"github.com/yewno/silver/loaders/federalReserve"
	"github.com/yewno/silver/loaders/taxonomyIndustry"
	"github.com/yewno/silver/loaders/taxonomyIsin"
	"github.com/yewno/silver/loaders/taxonomySp"
	"github.com/yewno/silver/loaders/news"
	"github.com/yewno/silver/loaders/uifs"
	"github.com/yewno/silver/loaders/unTrading"
	"github.com/yewno/silver/loaders/wikiPrices"
	"github.com/yewno/silver/loaders/worldGovernance"
	"github.com/yewno/log"
	"github.com/yewno/silver/utils"
	"time"
	"github.com/yewno/silver/loaders/deadTickers"
	"github.com/yewno/silver/loaders/isinUpdate"
	"github.com/yewno/silver/loaders/stockPrices"
	"io/ioutil"
	"fmt"
	"github.com/yewno/silver/loaders/companyIndustry"
	"github.com/yewno/silver/loaders/companyStandard"
	"github.com/yewno/silver/loaders/sec"
	"github.com/yewno/silver/loaders/secGov"
	"strings"
	"os"
	"sync"
	"github.com/yewno/silver/loaders/secGovMeta"
	"github.com/yewno/silver/loaders/newsUpdate"
)

type LoaderFn func(*services.ServiceContext, string) error

func LoadProcessor(source string) LoaderFn {
	switch source {
	case "company-customer":
		return companyCustomer.Process
	case "company-segment":
		return companySegment.Process
	case "company-shareholder":
		return companyShareholder.Process
	case "federal-reserve":
		return federalReserve.Process
	case "taxonomy-industry":
		return taxonomyIndustry.Process
	case "taxonomy-isin":
		return taxonomyIsin.Process
	case "taxonomy-sp":
		return taxonomySp.Process
	case "news":
		return news.Process
	case "uifs":
		return uifs.Process
	case "unTrading":
		return unTrading.Process
	case "wiki-prices":
		return wikiPrices.Process
	case "world-governance":
		return worldGovernance.Process
	case "dead-tickers":
		return deadTickers.Process
	case "isin-update":
		return isinUpdate.Process
	case "stock-prices":
		return stockPrices.Process
	case "company-industry":
		return companyIndustry.Process
	case "company-standard":
		return companyStandard.Process
	case "sec":
		return sec.Process
	case "sec-gov":
		return nil
	case "sec-gov-meta":
		return secGovMeta.Process
	case "news-update":
		return newsUpdate.Process
	}
	return nil
}

func getFiles() []string {
	var keys []string
	//files, err := ioutil.ReadDir("/Users/ziling/NYSE-DELISTED/")
	//if err != nil {
	//	log.Debugf(err.Error())
	//	return keys
	//}

	files2, err := ioutil.ReadDir("/Users/ziling/NYSE-DELISTED/")
	if err != nil {
		log.Debugf(err.Error())
		return keys
	}

	//for _, file := range files {
	//	key := fmt.Sprintf("/Users/ziling/Documents/NYSE/%s", file.Name())
	//	keys = append(keys, key)
	//}
	for _, file2 := range files2 {
		key2 := fmt.Sprintf("/Users/ziling/NYSE-DELISTED/%s", file2.Name())
		keys = append(keys, key2)
	}
	fmt.Println("%d", len(keys))
	return keys
}

func Load(source string, lFn LoaderFn, sctx *services.ServiceContext, flagTimeFrame time.Duration) error {

	switch source {
	case "company-customer":
		key := "company/cleaned/company_customer"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "company-segment":
		key := "company/cleaned/company_segment"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "company-shareholder":
		key := "company/cleaned/company_shareholder"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "federal-reserve":
		key := "federal-reserve/federal-reserve-code.json"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "taxonomy-industry", "news", "unTrading", "taxonomy-sp":
		keys := utils.RetrieveKeys(sctx, flagTimeFrame)
		for _, key := range keys {
			err := lFn(sctx, key)
			if err != nil {
				log.WithError(err)
				return err
			}
		}
	case "taxonomy-isin":
		key := "taxonomy-isin/isin2.json"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "uifs":
		key := "uifs/UIFS-datasets-codes.csv"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "world-governance":
		key := "world-governance/world_governance.csv"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "dead-tickers":
		key := "dead-tickers/missing_tickers.csv"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "wiki-prices":
		key := "/Users/ziling/Desktop/wikiprices.csv"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "isin-update":
		key := "/Users/ziling/Desktop/testing4"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "stock-prices":
		keys := getFiles()
		for _, key := range keys {
			err := lFn(sctx, key)
			if err != nil {
				log.WithError(err)
				return err
			}
		}
	case "company-industry":
		key := "/Users/ziling/Desktop/testing"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "company-standard":
		key := "/Users/ziling/Desktop/testing"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "sec":
		key := "/Users/ziling/Desktop/testing"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "sec-gov":
		sctx.Cfg.Bucket = "yewno-content-crawled"
		keys := utils.RetrieveKeys(sctx, flagTimeFrame)
		var counter int
		var counter2 int

		sourceInChan := make(chan string, 500000)
		for _, key := range keys {
			if strings.Contains(key, ".xml.gz") {
				counter2++
				//log.Infof("filtered %s", key)
				continue
			}

			if strings.Contains(key, ".pdf") {
				counter++
				//log.Infof("filtered %s", key)
				continue
			}
			sourceInChan <- key
		}
		close(sourceInChan)

		sourceOutChan := make(chan string, len(sourceInChan))

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int, inChan, outChan chan string, wg *sync.WaitGroup) {
				defer wg.Done()
				for key := range inChan {
					s, err := secGov.Process(sctx, key)
					log.Infof(s)
					if err != nil {
						log.WithError(err)
						continue
					}
					outChan <- s
				}
				//log.Infof("finished worker %d", id)
			}(i, sourceInChan, sourceOutChan, &wg)
		}
		wg.Wait()
		close(sourceOutChan)

		flagFile := "/Users/ziling/secMeta.txt"
		file, err := os.OpenFile(flagFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.WithError(err)
		}
		defer file.Close()

		for s := range sourceOutChan {
			_, err = file.WriteString(s)
			if err != nil {
				log.WithError(err)
				continue
			}
		}
		if _, err := file.Seek(0, 0); err != nil {
			log.WithError(err).Error("")
		}
		fmt.Println(counter)
		fmt.Println(counter2)
	case "sec-gov-meta":
		key := "/Users/ziling/secMeta.txt"
		err := lFn(sctx, key)
		if err != nil {
			log.WithError(err)
			return err
		}
	case "news-update":
		//keys := utils.RetrieveKeys(sctx, flagTimeFrame)
		//for _, key := range keys {
		    key := "/Users/ziling/Documents/news-update/TRNA.CMPNY_ALL.2008.31040047.txt"
			err := lFn(sctx, key)
			if err != nil {
				log.WithError(err)
				return err
			}
		//}//
	}

	return nil

}