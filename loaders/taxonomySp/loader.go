package taxonomySp

import (
	"fmt"
	"database/sql"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadData(db *sql.DB, record *formats.SP500, table string) error {

	_, err := db.Exec(InsertRecords(table),
		record.Isin,
		record.CompanyName,
		record.Ticker,
		"Stock",
		"NASDAQ",
	)
	if err != nil {
		log.Debugf("%v", record)
		log.Debugf(err.Error())
	}

	return err
}

func CreateTable(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (" +
			"isin varchar(255), " +
			"company_name varchar(255), " +
			"company_symbol varchar(255), " +
			"company_type varchar(255), " +
			"company_exchange varchar(255))", table)
}

func InsertRecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"isin, " +
		"company_name, " +
		"company_symbol, " +
		"company_type, " +
		"company_exchange) " +
		"values ($1, $2, $3, $4, $5)", table)
}
