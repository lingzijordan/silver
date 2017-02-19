package newsUpdate

import (
	"io/ioutil"
	"os"
	"io"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	"bufio"
	"strings"
	"crypto/sha1"
	"fmt"
	"time"
	"strconv"
)

func Process(sctx *services.ServiceContext, key string) error {
	tmpdir, err := ioutil.TempDir("/tmp", sctx.Cfg.Source)
	if err != nil {
		log.WithError(err).Error("unable to make tmp dir")
	}
	defer os.RemoveAll(tmpdir)

	//object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	//if err := sctx.Storage.Get(object); err != nil {
	//	log.WithError(err).Error("")
	//	return err
	//}
	//defer object.Close()

	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	//reader, err := gzip.NewReader(object.File) //object.File
	//if err != nil {
	//	log.WithError(err).Error("")
	//	return err
	//}
	scanner := bufio.NewScanner(f)

	metaMap := make(map[string]*formats.NewsUpdateMeta)
	tickerMap := make(map[string][]string)

	for scanner.Scan() {

		line := scanner.Text()
		arr := strings.Split(line, "\t")

		if arr[0] == "IDN_TIME" {
			continue
		}

		date := strings.Split(arr[42], "T")[0]
		dateArr := strings.Split(date, "-")

		year, _ := strconv.Atoi(dateArr[0])
		month, _ := strconv.Atoi(dateArr[1])
		day, _ := strconv.Atoi(dateArr[2])

		if month != 1 {continue}

		//if year < 2012 && month < 11 {
		//	continue
		//}

		hash := sha1.New()
		hashString := fmt.Sprintf("%s%s", "tr-news", arr[3])
		io.WriteString(hash, hashString)
		yid := fmt.Sprintf("%x", hash.Sum(nil))[0:32]

		lang := arr[41]
		if !strings.Contains(strings.ToLower(lang), "en") {
			continue
		}

		pnac := arr[28]
		metaMap[pnac] = &formats.NewsUpdateMeta{
			YId:             yid,
			Created:         time.Now().String(),
			Date:            arr[42],
			Day :            day,
			Month:           month,
			Year:            year,
			Language:        lang,
			Headline:        arr[26],
			Type:            "HEADLINE",
			IngestedAt:      time.Now().String(),
			Source:          "tr-news",
		}

		ticker := arr[2]
		tickers, ok := tickerMap[pnac]
		if !ok {
			tickerMap[pnac] = []string{ticker}
		} else {
			hasflag := false
			for _, t := range tickers {
				if t == ticker {hasflag = true}
			}
			if !hasflag {
				tickers = append(tickers, ticker)
				tickerMap[pnac] = tickers
			}
		}
	}

	for pnac, v := range metaMap {
		tickers, ok := tickerMap[pnac]
		if !ok {
			log.Debugf("no tickers for %s", pnac)
			continue
		} else {
			v.Tickers = strings.Join(tickers, ",")
		}
	}

	pairs, err := CacheNewsToS3(sctx, metaMap, tmpdir)
	if err != nil {
		log.WithError(err)
		return err
	}

	//insert metadata to dynamoDB
	err = UploadMetadata(metaMap)
	if err != nil {
		log.WithError(err)
		return err
	}

	//sending sqs messages now
	err = PushToQueue(sctx, pairs)
	if err != nil {
		log.WithError(err)
		return err
	}

	return nil
}