package companyIndustry

import (
	"database/sql"
	"fmt"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadData(db *sql.DB, record *formats.CompanyIndustry, table string) error {

	_, err := db.Exec(InsertRecords(table),
		record.ISIN,
		record.NACE,
		record.CP1,
		record.CP2,
		record.Section,
	)
	if err != nil {
		log.WithError(err)
	}

	return err
}

func CreateTable(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (" +
			"isin varchar(255), " +
			"class varchar(255), " +
			"grp varchar(255), " +
			"division varchar(255), " +
			"section varchar(255))", table)
}

func InsertRecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"isin, " +
		"class, " +
		"grp, " +
		"division, " +
		"section) " +
		"values ($1, $2, $3, $4, $5)", table)
}
