package worldGovernance

import (
	"fmt"
	"database/sql"
	"strconv"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func fillNull(a string) string {
	if a == "" {
		a = "0.00"
	}
	return a
}

func LoadData(db *sql.DB, record *formats.WorldGovernance, table string) error {
	//if record.CountryCode == nil {record.CountryCode = " "}
	//if record.CountryName == nil {record.CountryName = " "}
	//if record.IndicatorCode == nil {record.IndicatorCode = " "}
	//if record.IndicatorName == nil {record.IndicatorName = " "}

	year1996, _ := strconv.ParseFloat(fillNull(record.Year1996), 32)
	year1998, _ := strconv.ParseFloat(fillNull(record.Year1998), 32)
	year2000, _ := strconv.ParseFloat(fillNull(record.Year2000), 32)
	year2002, _ := strconv.ParseFloat(fillNull(record.Year2002), 32)
	year2003, _ := strconv.ParseFloat(fillNull(record.Year2003), 32)
	year2004, _ := strconv.ParseFloat(fillNull(record.Year2004), 32)
	year2005, _ := strconv.ParseFloat(fillNull(record.Year2005), 32)
	year2006, _ := strconv.ParseFloat(fillNull(record.Year2006), 32)
	year2007, _ := strconv.ParseFloat(fillNull(record.Year2007), 32)
	year2008, _ := strconv.ParseFloat(fillNull(record.Year2008), 32)
	year2009, _ := strconv.ParseFloat(fillNull(record.Year2009), 32)
	year2010, _ := strconv.ParseFloat(fillNull(record.Year2010), 32)
	year2011, _ := strconv.ParseFloat(fillNull(record.Year2011), 32)
	year2012, _ := strconv.ParseFloat(fillNull(record.Year2012), 32)
	year2013, _ := strconv.ParseFloat(fillNull(record.Year2013), 32)
	year2014, _ := strconv.ParseFloat(fillNull(record.Year2014), 32)
	year2015, _ := strconv.ParseFloat(fillNull(record.Year2015), 32)

	_, err := db.Exec(InsertRecords(table),
		record.CountryName,
		record.CountryCode,
		record.IndicatorName,
		record.IndicatorCode,
		year1996,
		year1998,
		year2000,
		year2002,
		year2003,
		year2004,
		year2005,
		year2006,
		year2007,
		year2008,
		year2009,
		year2010,
		year2011,
		year2012,
		year2013,
		year2014,
		year2015,
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
			"CountryName varchar(255), " +
			"CountryCode varchar(255), " +
			"IndicatorName varchar(255), " +
			"IndicatorCode varchar(255), " +
			"Year1996 decimal, " +
			"Year1998 decimal, " +
			"Year2000 decimal, " +
			"Year2002 decimal, " +
			"Year2003 decimal, " +
			"Year2004 decimal, " +
			"Year2005 decimal, " +
			"Year2006 decimal, " +
			"Year2007 decimal, " +
			"Year2008 decimal, " +
			"Year2009 decimal, " +
			"Year2010 decimal, " +
			"Year2011 decimal, " +
			"Year2012 decimal, " +
			"Year2013 decimal, " +
			"Year2014 decimal, " +
			"Year2015 decimal)", table)
}

func InsertRecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"CountryName, " +
		"CountryCode, " +
		"IndicatorName, " +
		"IndicatorCode, " +
		"Year1996, " +
		"Year1998, " +
		"Year2000, " +
		"Year2002, " +
		"Year2003, " +
		"Year2004, " +
		"Year2005, " +
		"Year2006, " +
		"Year2007, " +
		"Year2008, " +
		"Year2009, " +
		"Year2010, " +
		"Year2011, " +
		"Year2012, " +
		"Year2013, " +
		"Year2014, " +
		"Year2015) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)", table)
}
