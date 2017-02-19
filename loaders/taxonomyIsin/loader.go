package taxonomyIsin

import (
	"fmt"
	"database/sql"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadData(db *sql.DB, file *formats.ISIN, table string) error {
	tx, err := db.Begin()

	_, err = db.Exec(CreateTable(table), )
	if err != nil {
		log.WithError(err)
	}

	for _, data := range file.Data {
		for _, rec := range data.Mappings {
			_, err = db.Exec(InsertRecords(table),
				data.Isin,
				rec.Name,
				rec.Symbol,
				rec.Type,
				rec.Exchange)
			if err != nil {
				log.Debugf("%v", rec)
				log.Debugf(err.Error())
			}
		}
	}

	tx.Commit()

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
