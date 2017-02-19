package isinUpdate

import (
	"database/sql"
	"fmt"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func UpdateData(db *sql.DB, record *formats.IsinUpdate, table string) error {
	tx, err := db.Begin()

	_, err = db.Exec(InsertRecords(table),
		record.Isin,
		record.Ticker, )
	if err != nil {
		log.Debugf("%v", record)
		log.Debugf(err.Error())
	}

	tx.Commit()

	return err
}

func UpdateRecords(table string) string {
	return fmt.Sprintf("UPDATE %s " +
		"SET company_symbol=$1 " +
		"WHERE isin=$2 AND (company_symbol='') IS NOT FALSE", table)
}

func InsertRecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"isin, " +
		"company_symbol) " +
		"values ($1, $2)", table)
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
