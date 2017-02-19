package companyCustomer

import (
	"fmt"
	"database/sql"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadData(db *sql.DB, record *formats.Customer, table string) error {

	_, err := db.Exec(InsertRecords(table),
		record.CompanyName,
		record.CountryCodeIncorp,
		record.GICS,
		record.LatestAccountDate,
		record.Template,
		record.ConsCode,
		record.OperatingRev,
		record.NumberEmployee,
		record.Indep,
		record.ISIN,
		record.MajorCustomersDate,
		record.MajorCustomersName,
		record.MajorCustomersRevenue,
		record.MajorCustomersRevenuePer,
		record.MajorCustomersUnit,
		record.MajorCustomersCurrency,
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
			"company_name varchar(255), " +
			"country_code_incorp varchar(255), " +
			"gics_code varchar(255), " +
			"latest_account_date varchar(255), " +
			"template varchar(255), " +
			"cons_code varchar(255), " +
			"operating_rev_turnover_th_usd_last_avail_yr varchar(255), " +
			"number_of_employees_last_avail_yr varchar(255), " +
			"indep_ind varchar(255), " +
			"isin_number varchar(255), " +
			"major_customers_date varchar(255), " +
			"major_customers_name varchar(255), " +
			"major_customers_revenue_usd decimal, " +
			"major_customers_revenue_percentage decimal, " +
			"major_customers_unit varchar(255), " +
			"major_customers_currency varchar(255))", table)
}

func InsertRecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"company_name, " +
		"country_code_incorp, " +
		"gics_code, " +
		"latest_account_date, " +
		"template, " +
		"cons_code, " +
		"operating_rev_turnover_th_usd_last_avail_yr, " +
		"number_of_employees_last_avail_yr, " +
		"indep_ind, " +
		"isin_number, " +
		"major_customers_date, " +
		"major_customers_name, " +
		"major_customers_revenue_usd, " +
		"major_customers_revenue_percentage, " +
		"major_customers_unit, " +
		"major_customers_currency) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)", table)
}
