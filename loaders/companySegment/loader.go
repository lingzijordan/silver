package companySegment

import (
	"fmt"
	"database/sql"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadData(db *sql.DB, record *formats.CompanySegment, table string) error {

	_, err := db.Exec(InsertRecords(table),
		record.CompanyName,
		record.CountryCodeIncorp,
		record.GICS,
		record.LatestDate,
		record.Template,
		record.ConsCode,
		record.OperatingRev,
		record.NumEmployee,
		record.Indep,
		record.ISIN,
		record.Date,
		record.Label,
		record.Sales,
		record.Profit,
		record.Assets,
		record.Depreciation,
		record.Ppe,
		record.Randd,
		record.CapitalExpenditure,
		record.GrossPremiumthusd,
		record.GrossPremium,
		record.NetPremiumthusd,
		record.NetPremium,
		record.Unit,
		record.Currency,
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
			"operating_rev_turnover_th_usd_last_avail_yr decimal, " +
			"number_of_employees_last_avail_yr decimal, " +
			"indep_ind varchar(255), " +
			"isin_number varchar(255), " +
			"geographic_date varchar(255), " +
			"geographic_label varchar(255), " +
			"geographic_sales_th_usd decimal, " +
			"geographic_profit_th_usd decimal, " +
			"geographic_assets_th_usd decimal, " +
			"geographic_depreciation_th_usd decimal, " +
			"geographic_ppe_th_usd decimal, " +
			"geographic_randd_th_usd decimal, " +
			"geographic_capital_expenditure_th_usd decimal, " +
			"geographic_gross_premium_th_usd decimal, " +
			"geographic_gross_premium_percentage decimal, " +
			"geographic_net_premium_th_usd decimal, " +
			"geographic_net_premium_percentage decimal, " +
			"geographic_init varchar(255), " +
			"geographic_currency varchar(255))", table)
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
		"geographic_date, " +
		"geographic_label, " +
		"geographic_sales_th_usd, " +
		"geographic_profit_th_usd, " +
		"geographic_assets_th_usd, " +
		"geographic_depreciation_th_usd, " +
		"geographic_ppe_th_usd, " +
		"geographic_randd_th_usd, " +
		"geographic_capital_expenditure_th_usd, " +
		"geographic_gross_premium_th_usd, " +
		"geographic_gross_premium_percentage, " +
		"geographic_net_premium_th_usd, " +
		"geographic_net_premium_percentage, " +
		"geographic_init, " +
		"geographic_currency) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)", table)
}