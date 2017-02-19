package taxonomyIndustry

import (
	"fmt"
	"database/sql"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadData(db *sql.DB, file *formats.Industries, table string) error {
	tx, err := db.Begin()

	_, err = db.Exec(CreateTable(table), )
	if err != nil {
		log.WithError(err)
	}

	for _, data := range file.Leaders {
		_, err = db.Exec(InsertRecords(table),
			data.Name,
			data.Symbol,
			data.Value,
			file.Industry,
			"Leaders")
		if err != nil {
			log.Debugf("Inserting record error table %s", table)
		}
	}

	for _, data := range file.Laggards {
		_, err = db.Exec(InsertRecords(table),
			data.Name,
			data.Symbol,
			data.Value,
			file.Industry,
			"Laggards")
		if err != nil {
			log.Debugf("Inserting record error table %s", table)
		}
	}

	for _, data := range file.Laggards {
		_, err = db.Exec(InsertRecords(table),
			data.Name,
			data.Symbol,
			"null",
			file.Industry,
			"null")
		if err != nil {
			log.Debugf("Inserting record error table %s", table)
		}
	}

	tx.Commit()

	return err
}

func CreateTable(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (" +
			"company_name varchar(255), " +
			"company_symbol varchar(255), " +
			"company_value varchar(255), " +
			"industry varchar(255), " +
			"company_position varchar(255))", table)
}

func InsertRecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"company_name, " +
		"company_symbol, " +
		"company_value, " +
		"industry, " +
		"company_position) " +
		"values ($1, $2, $3, $4, $5)", table)
}