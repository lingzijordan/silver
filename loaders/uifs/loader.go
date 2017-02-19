package uifs

import (
	"fmt"
	"bytes"
	"strings"
	"math"
	"database/sql"
	"strconv"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
)

func fillNil(i string) string {
	if i == "<nil>" {
		i = "0.00"
	}
	return i
}

func ParseNum(i string) float64 {
	i = strings.Replace(i, "]", "", -1)
	if !strings.Contains(i, "e+") {
		num, err := strconv.ParseFloat(fillNil(i), 32)
		if err != nil {
			log.Debugf(err.Error())
		}
		return num
	} else {
		arr := strings.Split(i, "e+")
		first, err := strconv.ParseFloat(arr[0], 32)
		if err != nil {
			log.Debugf(err.Error())
		}
		second, err := strconv.Atoi(arr[1])
		if err != nil {
			log.Debugf(err.Error())
		}

		//fmt.Printf("%f %d\n", first, second)

		return first * float64(math.Pow10(second))
	}
}

func LoadData(db *sql.DB, file *formats.UifsJson, table string) error {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(CreaTetable(table, file.Dataset.ColumnNames), )
	if err != nil {
		log.Debugf(table)
		log.Debugf(strings.Join(file.Dataset.ColumnNames, ","))
		log.Debugf(err.Error())
	}

	for _, record := range file.Dataset.Data {
		//fmt.Println(record)
		s := fmt.Sprint(record)
		arr := strings.Split(s, " ")

		date := strings.Replace(arr[0], "[", "", -1)
		numbers := arr[1:]
		var floatNumbers []float64
		for _, i := range numbers {
			num := ParseNum(i)
			floatNumbers = append(floatNumbers, num)
		}

		new := make([]interface{}, len(floatNumbers)+1)
		for i, v := range floatNumbers {
			new[i+1] = v
		}
		new[0] = date
		//
		//fmt.Println(uifs.InsertRecords(table, file.Dataset.ColumnNames))
		_, err = db.Exec(InsertRecords(table, file.Dataset.ColumnNames), new...)
		if err != nil {
			log.Debugf(table)
			log.Debugf(strings.Join(file.Dataset.ColumnNames, ","))
			log.Debugf(err.Error())
		}
	}

	tx.Commit()

	return nil
}

func CreaTetable(table string, schema []string) string {

	var buffer bytes.Buffer
	header := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", table)
	buffer.WriteString(header)

	for i, column := range schema {
		column = strings.Replace(column, " ", "_", -1)
		column = strings.Replace(column, "(", "", -1)
		column = strings.Replace(column, ")", "", -1)
		column = strings.Replace(column, ".", "", -1)
		column = strings.Replace(column, "-", "_", -1)
		column = strings.Replace(column, "&", "_", -1)
		column = strings.Replace(column, ";", "_", -1)
		column = strings.Replace(column, ":", "_", -1)
		column = strings.Replace(column, "/", "_", -1)
		column = strings.Replace(column, "%", "_", -1)
		column = strings.Replace(column, "[", "", -1)
		column = strings.Replace(column, "]", "", -1)
		column = strings.Replace(column, "+", "_", -1)
		column = strings.Replace(column, "=", "_", -1)
		column = strings.Replace(column, "'", "", -1)
		column = strings.Replace(column, "13", "thirteen", -1)
		column = strings.Replace(column, "3", "three", -1)
		column = strings.Replace(column, "12", "twelve", -1)
		column = strings.Replace(column, "*", "_", -1)
		column = strings.Replace(column, "{", "_", -1)
		column = strings.Replace(column, "6", "six", -1)
		column = strings.Replace(column, "<", "less_than", -1)
		column = strings.Replace(column, "88", "eighty_eight", -1)
		column = strings.Replace(column, ">", "larger_than", -1)
		column = strings.Replace(column, "5", "five", -1)
		column = strings.Replace(column, "#", "_", -1)
		column = strings.Replace(column, "2", "two", -1)
		column = strings.Replace(column, "1", "one", -1)
		column = strings.Replace(column, "24", "twenty_four", -1)
		column = strings.Replace(column, "}", "_", -1)
		column = strings.Replace(column, "7", "seven", -1)
		column = strings.Replace(column, "9", "nine", -1)

		var field string
		if i == 0 {
			field = fmt.Sprintf("%s varchar(255), ", column)
		} else if i == len(schema)-1 {
			field = fmt.Sprintf("%s decimal)", column)
		} else {
			field = fmt.Sprintf("%s decimal, ", column)
		}
		buffer.WriteString(field)
	}

	return buffer.String()
}

func InsertRecords(table string, schema []string) string {

	var buffer bytes.Buffer
	var buffer2 bytes.Buffer
	header := fmt.Sprintf("INSERT INTO %s (", table)
	buffer.WriteString(header)

	for i, column := range schema {
		column = strings.Replace(column, " ", "_", -1)
		column = strings.Replace(column, "(", "", -1)
		column = strings.Replace(column, ")", "", -1)
		column = strings.Replace(column, ".", "", -1)
		column = strings.Replace(column, "-", "_", -1)
		column = strings.Replace(column, "&", "_", -1)
		column = strings.Replace(column, ";", "_", -1)
		column = strings.Replace(column, ":", "_", -1)
		column = strings.Replace(column, "/", "_", -1)
		column = strings.Replace(column, "%", "_", -1)
		column = strings.Replace(column, "[", "", -1)
		column = strings.Replace(column, "]", "", -1)
		column = strings.Replace(column, "+", "_", -1)
		column = strings.Replace(column, "=", "_", -1)
		column = strings.Replace(column, "'", "", -1)
		column = strings.Replace(column, "13", "thirteen", -1)
		column = strings.Replace(column, "3", "three", -1)
		column = strings.Replace(column, "12", "twelve", -1)
		column = strings.Replace(column, "*", "_", -1)
		column = strings.Replace(column, "{", "_", -1)
		column = strings.Replace(column, "6", "six", -1)
		column = strings.Replace(column, "<", "less_than", -1)
		column = strings.Replace(column, "88", "eighty_eight", -1)
		column = strings.Replace(column, ">", "larger_than", -1)
		column = strings.Replace(column, "5", "five", -1)
		column = strings.Replace(column, "#", "_", -1)
		column = strings.Replace(column, "2", "two", -1)
		column = strings.Replace(column, "1", "one", -1)
		column = strings.Replace(column, "24", "twenty_four", -1)
		column = strings.Replace(column, "}", "_", -1)
		column = strings.Replace(column, "7", "seven", -1)
		column = strings.Replace(column, "9", "nine", -1)

		var value string
		var s string
		if i == 0{
			value = fmt.Sprintf("%s, ", column)
			s = fmt.Sprintf("($%d, ", i+1)
		} else if i == len(schema)-1 {
			value = fmt.Sprintf("%s) values ", column)
			s = fmt.Sprintf("$%d) ", i+1)
		} else {
			value = fmt.Sprintf("%s, ", column)
			s = fmt.Sprintf("$%d, ", i+1)
		}
		buffer.WriteString(value)
		buffer2.WriteString(s)
	}

	buffer.WriteString(buffer2.String())

	return buffer.String()
}
