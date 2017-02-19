package wikiPrices

import (
	"fmt"
	"database/sql"
	"strconv"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

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
		log.WithError(err)
	}

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
