package companyStandard

import (
	"database/sql"
	"fmt"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadData(db *sql.DB, record *formats.CompanyStandard, table string) error {

	_, err := db.Exec(InsertRecords(table),
		record.CompanyName,
		record.Isin,
		record.Gics,
		record.GicsDesc,
		record.Naics,
		record.NaicsDesc,
		record.USSic,
		record.USSicDesc,
		record.Nace,
		record.NaceDesc,
	)
	if err != nil {
		log.WithError(err)
	}

	return err
}

func CreateTable(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (" +
			"company_name varchar(255), " +
			"isin varchar(255), " +
			"gics_code varchar(255), " +
			"gics_description varchar(255), " +
			"naics_2012_cord_code varchar(255), " +
			"naics_2012_core_code_description varchar(255), " +
	        "us_sic_core_code varchar(255), " +
		    "us_sic_core_code_description varchar(255), " +
			"nace_rev_2_core_code varchar(255), " +
		    "nace_rev_2_core_code_description varchar(255))", table)
}

func InsertRecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"company_name, " +
		"isin, " +
		"gics_code, " +
		"gics_description, " +
		"naics_2012_cord_code, " +
		"naics_2012_core_code_description, " +
		"us_sic_core_code, " +
		"us_sic_core_code_description, " +
		"nace_rev_2_core_code, " +
		"nace_rev_2_core_code_description) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", table)
}