package deadTickers

import (
	"fmt"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"database/sql"
	"strconv"
	"github.com/yewno/silver/formats"
	"time"
	"encoding/csv"
	"io"
	"sync"
	"github.com/yewno/silver/utils"
)

func LoadRecords(sctx *services.ServiceContext, tickers chan string) error {
	sourceInChan := make(chan *formats.WikiPrices, 500000)

	for ticker := range tickers {
		key := fmt.Sprintf("%s/%s/%s.csv", sctx.Cfg.Source, time.Now().Format("20060102"), ticker)
		object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
		if err := sctx.Storage.Get(object); err != nil {
			log.WithError(err).Error("")
			return err
		}


		r := csv.NewReader(object.File) //object.File
		r.Comma = ','

		for {
			var record formats.Prices
			err := utils.CsvUnmarshal(r, &record)
			log.Infof("%v", record)
			if err != nil {
				log.Debugf(err.Error())
			}
			if err == io.EOF {
				break
			}
			if record.Date == "Date" || record.Date == "" {
				continue
			}

			wikiPrice := &formats.WikiPrices{
				Ticker: ticker,
				Date: record.Date,
				Open: record.Open,
				High: record.High,
				Low: record.Low,
				Close: record.Close,
				Volume: record.Volume,
				AdjClose: record.AdjClose,
			}

			sourceInChan <- wikiPrice

		}
		log.Infof("ticker %s being read! ", ticker)
		object.Close()
	}

	close(sourceInChan)
	log.Infof("%d", len(sourceInChan))

	tx, err := sctx.DB.Begin()

	_, err = sctx.DB.Exec(CreateTable(sctx.Cfg.DBtable), )
	if err != nil {
		log.WithError(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.WikiPrices, table string, wg *sync.WaitGroup) {
			defer wg.Done()
			for record := range inChan {

				err = LoadData(db, record, table)
				if err != nil {
					log.WithError(err)
				}
			}
			log.Infof("finished worker %d", id)
		}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &wg)
	}
	wg.Wait()

	tx.Commit()

	return err
}

func CreateTable(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (" +
			"Ticker varchar(255), " +
			"Date varchar(255), " +
			"Open decimal, " +
			"High decimal, " +
			"Low decimal, " +
			"Close decimal, " +
			"Volume decimal, " +
			"ExDividend decimal, " +
			"SplitRatio decimal, " +
			"AdjOpen decimal, " +
			"AdjHigh decimal, " +
			"AdjLow decimal, " +
			"AdjClose decimal, " +
			"AdjVolume decimal)", table)
}

func RemoveRecords(sctx *services.ServiceContext, ticker string) error {
	tx, err := sctx.DB.Begin()

	stmt := fmt.Sprintf("DELETE FROM %s WHERE ticker=$1", sctx.Cfg.DBtable)

	_, err = sctx.DB.Exec(stmt, ticker)
	if err != nil {
		log.Debugf(err.Error())
	}

	tx.Commit()

	return err
}

func LoadData(db *sql.DB, record *formats.WikiPrices, table string) error {

	open, _ := strconv.ParseFloat(record.Open, 64)
	high, _ := strconv.ParseFloat(record.High, 64)
	low, _ := strconv.ParseFloat(record.Low, 64)
	close1, _ := strconv.ParseFloat(record.Close, 64)
	volume, _ := strconv.ParseFloat(record.Volume, 64)
	exDividend, _ := strconv.ParseFloat(record.ExDividend, 64)
	splitRatio, _ := strconv.ParseFloat(record.SplitRatio, 64)
	adjOpen, _ := strconv.ParseFloat(record.AdjOpen, 64)
	adjHigh, _ := strconv.ParseFloat(record.AdjHigh, 64)
	adjLow, _ := strconv.ParseFloat(record.AdjLow, 64)
	adjClose, _ := strconv.ParseFloat(record.AdjClose, 64)
	adjVolume, _ := strconv.ParseFloat(record.AdjVolume, 64)

	_, err := db.Exec(InsertRecords(table),
		record.Ticker,
		record.Date,
		open,
		high,
		low,
		close1,
		volume,
		exDividend,
		splitRatio,
		adjOpen,
		adjHigh,
		adjLow,
		adjClose,
		adjVolume,
	)
	if err != nil {
		log.Debugf("%v", record)
		log.Debugf(err.Error())
	}

	return err
}

func InsertRecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"Ticker, " +
		"Date, " +
		"Open, " +
		"High, " +
		"Low, " +
		"Close, " +
		"Volume, " +
		"ExDividend, " +
		"SplitRatio, " +
		"AdjOpen, " +
		"AdjHigh, " +
		"AdjLow, " +
		"AdjClose, " +
		"AdjVolume) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", table)
}