package companyShareholder

import (
	"fmt"
	"database/sql"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadData(db *sql.DB, record *formats.Shareholders, table string) error {

	_, err := db.Exec(InsertRecords(table),
		record.CompanyName,
		record.CountryCode,
		record.GICSCode,
		record.LatestAccountDate,
		record.Template,
		record.ConsCode,
		record.Operating,
		record.NumberOfEmployee,
		record.IndepInd,
		record.ISIN,
		record.BvD,
		record.Comments,
		record.RecordedShareholders,
		record.Name,
		record.Salutation,
		record.FirstName,
		record.LastName,
		record.BvDIDNumber,
		record.TickerSymbol,
		record.CountryISOCode,
		record.City,
		record.Type,
		record.NACECoreCode,
		record.NACEText,
		record.NAICS2012CoreCode,
		record.NAICS2012Text,
		record.Direct,
		record.Total,
		record.InformationOnPossibleChange,
		record.InformationSource,
		record.InformationDate,
		record.OperatingRevenue,
		record.TotalAssets,
		record.NoOfEmployees,
	)
	if err != nil {
		log.Debugf("%v", record)
		//log.Debugf(err.Error())
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
			"bvd_indep_indic varchar(255), " +
			"shareholder_comments_text text, " +
			"no_of_recorded_shareholders decimal, " +
			"shareholder_name varchar(255), " +
			"shareholder_salutation varchar(255), " +
			"shareholder_first_name varchar(255), " +
			"shareholder_last_name varchar(255), " +
			"shareholder_bvd_id_number varchar(255), " +
			"shareholder_ticker_symbol varchar(255), " +
			"shareholder_country_iso_code varchar(255), " +
			"shareholder_city varchar(255), " +
			"shareholder_type varchar(255), " +
			"shareholder_nace_rev_2_core_code varchar(255), " +
			"shareholder_nace_rev_2_text_description text, " +
			"shareholder_naics_2012_core_code varchar(255), " +
			"shareholder_naics_2012_text_description text, " +
			"shareholder_direct_percentage decimal, " +
			"shareholder_total_percentage decimal, " +
			"shareholder_information_on_possible_change_in_percentage decimal, " +
			"shareholder_information_source varchar(255), " +
			"shareholder_information_date varchar(255), " +
			"shareholder_operating_revenue_turnover_m_usd decimal, " +
			"shareholder_total_assets_m_usd decimal, " +
			"shareholder_number_of_employees decimal)", table)
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
		"bvd_indep_indic, " +
		"shareholder_comments_text, " +
		"no_of_recorded_shareholders, " +
		"shareholder_name, " +
		"shareholder_salutation, " +
		"shareholder_first_name, " +
		"shareholder_last_name, " +
		"shareholder_bvd_id_number, " +
		"shareholder_ticker_symbol, " +
		"shareholder_country_iso_code, " +
		"shareholder_city, " +
		"shareholder_type, " +
		"shareholder_nace_rev_2_core_code, " +
		"shareholder_nace_rev_2_text_description, " +
		"shareholder_naics_2012_core_code, " +
		"shareholder_naics_2012_text_description, " +
		"shareholder_direct_percentage, " +
		"shareholder_total_percentage, " +
		"shareholder_information_on_possible_change_in_percentage, " +
		"shareholder_information_source, " +
		"shareholder_information_date, " +
		"shareholder_operating_revenue_turnover_m_usd, " +
		"shareholder_total_assets_m_usd, " +
		"shareholder_number_of_employees) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34)", table)
}