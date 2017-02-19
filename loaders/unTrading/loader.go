package unTrading

import (
	"fmt"
	"database/sql"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func LoadDataEB02(db *sql.DB, record *formats.UnBulk, table string) error {

	_, err := db.Exec(InsertRecordsEB02(table),
		record.Classification,
		record.Year,
		record.Period,
		record.PeriodDesc,
		record.AggregateLevel,
		record.IsLeafCode,
		record.TradeFlowCode,
		record.TradeFlow,
		record.ReporterCode,
		record.Reporter,
		record.ReporterISO,
		record.PartnerCode,
		record.Partner,
		record.PartnerISO,
		record.CommodityCode,
		record.Commodity,
		record.TradeValueUS,
		record.Flag, )
	if err != nil {
		//log.Debugf("Inserting record error table %s", table)
		log.WithError(err)
	}
	//}

	return err
}

func LoadDataHS(db *sql.DB, record *formats.UnBulkHS, table string) error {

	_, err := db.Exec(InsertRecordsHS(table),
		record.Classification,
		record.Year,
		record.Period,
		record.PeriodDesc,
		record.AggregateLevel,
		record.IsLeafCode,
		record.TradeFlowCode,
		record.TradeFlow,
		record.ReporterCode,
		record.Reporter,
		record.ReporterISO,
		record.PartnerCode,
		record.Partner,
		record.PartnerISO,
		record.CommodityCode,
		record.Commodity,
		record.QtyUnitCode,
		record.QtyUnit,
		record.Qty,
		record.NetweightKg,
		record.TradeValueUS,
		record.Flag, )
	if err != nil {
		//log.Debugf("Inserting record error table %s", table)
		log.WithError(err)
	}
	//}

	return err
}

func CreateTable(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (" +
			"Classification varchar(255), " +
			"Year varchar(255), " +
			"Period varchar(255), " +
			"PeriodDesc varchar(255), " +
			"AggregateLevel varchar(255), " +
			"IsLeafCode varchar(255), " +
			"TradeFlowCode varchar(255), " +
			"TradeFlow varchar(255), " +
			"ReporterCode varchar(255), " +
			"Reporter varchar(255), " +
			"ReporterISO varchar(255), " +
			"PartnerCode varchar(255), " +
			"Partner varchar(255), " +
			"PartnerISO varchar(255), " +
			"CommodityCode varchar(255), " +
			"Commodity text, " +
			"QtyUnitCode varchar(255), " +
			"QtyUnit varchar(255), " +
			"Qty varchar(255), " +
			"NetweightKg varchar(255), " +
			"TradeValueUS varchar(255), " +
			"Flag varchar(255))", table)
}

func InsertRecordsEB02(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"Classification, " +
		"Year, " +
		"Period, " +
		"PeriodDesc, " +
		"AggregateLevel, " +
		"IsLeafCode, " +
		"TradeFlowCode, " +
		"TradeFlow, " +
		"ReporterCode, " +
		"Reporter, " +
		"ReporterISO, " +
		"PartnerCode, " +
		"Partner, " +
		"PartnerISO, " +
		"CommodityCode, " +
		"Commodity, " +
		"TradeValueUS, " +
		"Flag) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)", table)
}

func InsertRecordsHS(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"Classification, " +
		"Year, " +
		"Period, " +
		"PeriodDesc, " +
		"AggregateLevel, " +
		"IsLeafCode, " +
		"TradeFlowCode, " +
		"TradeFlow, " +
		"ReporterCode, " +
		"Reporter, " +
		"ReporterISO, " +
		"PartnerCode, " +
		"Partner, " +
		"PartnerISO, " +
		"CommodityCode, " +
		"Commodity, " +
		"QtyUnitCode, " +
		"QtyUnit, " +
		"Qty, " +
		"NetweightKg, " +
		"TradeValueUS, " +
		"Flag) " +
		"values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)", table)
}
