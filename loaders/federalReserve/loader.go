package federalReserve

import (
	"fmt"
	"database/sql"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func Createtable(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (" +
			"RealtimeStart varchar(255), " +
			"RealtimeEnd varchar(255), " +
			"Date varchar(255), " +
			"Value varchar(255))", table)
}

func Insertrecords(table string) string {
	return fmt.Sprintf("Insert into %s ( " +
		"RealtimeStart, " +
		"RealtimeEnd, " +
		"Date, " +
		"Value) " +
		"values ($1, $2, $3, $4)", table)
}

func LoadData(db *sql.DB, file *formats.FRJson, table string) error {
	tx, err := db.Begin()

	_, err = db.Exec(Createtable(table), )
	if err != nil {
		log.WithError(err)
	}

	if len(file.Observations) == 0 {
		log.Debugf("series code %s has 0 record!", table)
	} else {
		log.Infof("number of records for %s is: %d", table, len(file.Observations))
	}

	for _, data := range file.Observations {
		_, err = db.Exec(Insertrecords(table),
			data.RealtimeStart,
			data.RealtimeEnd,
			data.Date,
			data.Value)
		if err != nil {
			log.Debugf(table)
			log.Debugf(err.Error())
		}
	}
	//}

	tx.Commit()

	return err
}
