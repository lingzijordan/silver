package stockPrices

import (
	"fmt"
	"database/sql"
	"strconv"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadData(db *sql.DB, record *formats.StockPricesTable, table string) error {

	open, _ := strconv.ParseFloat(record.Open, 64)
	high, _ := strconv.ParseFloat(record.High, 64)
	low, _ := strconv.ParseFloat(record.Low, 64)
	close1, _ := strconv.ParseFloat(record.Close, 64)
	volume, _ := strconv.ParseFloat(record.Volume, 64)

	_, err := db.Exec(InsertRecords(table),
		record.Ticker,
		record.Date,
		open,
		high,
		low,
		close1,
		volume,
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
			"adjclose decimal, " +
			"Volume decimal)", table)
}

func InsertRecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"Ticker, " +
		"Date, " +
		"Open, " +
		"High, " +
		"Low, " +
		"adjclose, " +
		"Volume) " +
		"values ($1, $2, $3, $4, $5, $6, $7)", table)
}
