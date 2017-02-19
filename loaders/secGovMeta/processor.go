package secGovMeta

import (
	"strings"
	"os"
	"bufio"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	"path"
	"time"
	"strconv"
	"fmt"
	"encoding/json"
	"github.com/yewno/silver/utils"
	"sync"
)

type TickerIsin struct {
	Ticker string
	Isin   string
}

func getCikMapping() map[string]*TickerIsin {
	mapping := make(map[string]*TickerIsin)
	key := "/Users/ziling/resultMap.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		cik := arr[0]
		ticker := arr[1]
		isin := arr[2]

		mapping[cik] = &TickerIsin{
			Ticker: ticker,
			Isin: isin,
		}
	}

	return mapping
}


func CreateMetaDataFile(sctx *services.ServiceContext, meta *formats.SecContentMeta) error {
	byteArr, err := json.Marshal(meta)
	if err != nil {
		log.Debugf(err.Error())
		return err
	}
	key := fmt.Sprintf("%s/%s.json", "sec-gov", meta.YId)
	err = utils.SavetoS3CustomKey(byteArr, "yewno-finance", key, sctx)
	if err != nil {
		log.Debugf(key)
		log.Debugf(err.Error())
		return err
	}
	log.Infof(key)

	return nil
}

func Process(sctx *services.ServiceContext, key string) error {
	metaInfo := make(map[string]*formats.SecContentMeta)
	cikMapping := getCikMapping()
	msgMap := make(map[string]string)

	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		key := arr[1]
		source := path.Dir(key)
		fname := path.Base(key)
		base := strings.Split(fname, ".")[0]
		arr = strings.Split(base, "-")
		cik := arr[0]
		year, _ := strconv.Atoi(arr[1])
		month, _ := strconv.Atoi(arr[2])
		day, _ := strconv.Atoi(arr[3])
		hashcode := arr[4]
		ingestedAt := time.Now().String()

		date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)

		meta := &formats.SecContentMeta{
			YId: yid,
			Date: date.Format(time.RFC3339),
			Day: day,
			Month: month,
			Year: year,
			Type: "FILLING",
			Language: "eng",
			IngestedAt: ingestedAt,
			Source: source,
			Title: fname,
			Cik: cik,
			Hashcode: hashcode,
		}

		v, ok := cikMapping[cik]
		if ok {
			meta.Ticker = v.Ticker
			meta.Isin = v.Isin
		}

		loc := fmt.Sprintf("%s/%s.txt", source, yid)
		msgMap[yid] = loc

		metaInfo[yid] = meta
	}

	//var counter int
	sourceInChan := make(chan *formats.SecContentMeta, 250000)
	for _, meta := range metaInfo {
		sourceInChan <- meta
	}
	close(sourceInChan)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int, inChan chan *formats.SecContentMeta, wg *sync.WaitGroup) {
			defer wg.Done()
			for m := range inChan {
				err := CreateMetaDataFile(sctx, m)
				if err != nil {
					log.WithError(err)
					continue
				}
			}
			//log.Infof("finished worker %d", id)
		}(i, sourceInChan, &wg)
	}
	wg.Wait()

	//for _, meta := range metaInfo {
	//	counter++
	//	err := CreateMetaDataFile(sctx, meta)
	//	if err != nil {
	//		log.Debugf(err.Error())
	//		return err
	//	}
	//
	//	if counter%1000 == 0 {
	//		fmt.Println("%d", counter)
	//	}
	//}

	//err = UploadMetadata(metaInfo)
	//if err != nil {
	//	log.WithError(err)
	//}
	//
	//err = PushToQueue(sctx, msgMap)
	//if err != nil {
	//	log.WithError(err)
	//}

	return nil
}
