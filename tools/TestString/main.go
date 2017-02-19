package main

import (
	"fmt"
	"bytes"
	"strings"
	"time"
	"os"
	"io/ioutil"
	"encoding/json"
	"database/sql"
	"strconv"
	_ "github.com/lib/pq"
	"github.com/yewno/log"
     "math"
	"github.com/yewno/log/handlers/text"
)

type Uifs struct {
	Dataset struct {
				ID int `json:"id"`
				DatasetCode string `json:"dataset_code"`
				DatabaseCode string `json:"database_code"`
				Name string `json:"name"`
				Description string `json:"description"`
				RefreshedAt time.Time `json:"refreshed_at"`
				NewestAvailableDate string `json:"newest_available_date"`
				OldestAvailableDate string `json:"oldest_available_date"`
				ColumnNames []string `json:"column_names"`
				Frequency string `json:"frequency"`
				Type string `json:"type"`
				Premium bool `json:"premium"`
				Limit interface{} `json:"limit"`
				Transform interface{} `json:"transform"`
				ColumnIndex interface{} `json:"column_index"`
				StartDate string `json:"start_date"`
				EndDate string `json:"end_date"`
				Data []interface{} `json:"data"`
				Collapse interface{} `json:"collapse"`
				Order interface{} `json:"order"`
				DatabaseID int `json:"database_id"`
			} `json:"dataset"`
}

func Createtable(table string, schema []string) string {

	var buffer bytes.Buffer
	header := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", table)
	buffer.WriteString(header)

	for i, column := range schema {
		column = strings.Replace(column, " ", "_", -1)
		column = strings.Replace(column, "(", "", -1)
		column = strings.Replace(column, ")", "", -1)
		column = strings.Replace(column, ".", "", -1)
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


func Insertrecords(table string, schema []string) string {

	var buffer bytes.Buffer
	var buffer2 bytes.Buffer
	header := fmt.Sprintf("INSERT INTO %s (", table)
	buffer.WriteString(header)

	for i, column := range schema {
		column = strings.Replace(column, " ", "_", -1)
		column = strings.Replace(column, "(", "", -1)
		column = strings.Replace(column, ")", "", -1)
		column = strings.Replace(column, ".", "", -1)
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

func fillNull(i string) string {
	if i == "<nil>" {
		i = "0.00"
	}
	return i
}

func ParseNum(i string) float64 {
	i = strings.Replace(i, "]", "", -1)
	if !strings.Contains(i, "e+") {
		num, err := strconv.ParseFloat(fillNull(i), 32)
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

		fmt.Printf("%f %d\n", first, second)

		return first * float64(math.Pow10(second))
	}
}

func main() {
	log.SetHandler(text.Default)
	log.SetLevel(log.DebugLevel)

	f, err := os.Open("/Users/ziling/Desktop/testing.json")
	if err != nil {
		panic(err)
	}
	arrayByte, _ := ioutil.ReadAll(f)
	file := new(Uifs)
	if err = json.Unmarshal(arrayByte, file); err != nil {
		panic(err)
	}

	fmt.Println(file.Dataset.ColumnNames)
	fmt.Println()
	fmt.Println()


	db, err := sql.Open("postgres",
		"postgres://root:dnYq6KXK@finance.chhasmcyzmnj.us-west-2.rds.amazonaws.com:5432/uifs_macro?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("pinging mysql is good!\n")
	}
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(Createtable("testing", file.Dataset.ColumnNames))
	if err != nil {
		panic(err)
	}

	for _, record := range file.Dataset.Data {
		fmt.Println(record)
		s := fmt.Sprint(record)
		arr := strings.Split(s, " ")

		date := strings.Replace(arr[0], "[", "", -1)
		fmt.Println(date)
		numbers := arr[1:]
		var floatNumbers []float64
		for _, i := range numbers {

			fmt.Printf("before parse %s\n", i)
			num := ParseNum(i)
			fmt.Printf("%f\n",num)
			floatNumbers = append(floatNumbers, num)
		}

		new := make([]interface{}, len(floatNumbers)+1)
		for i, v := range floatNumbers {
			new[i+1] = v
		}
		new[0] = date
		//
		fmt.Println(Insertrecords("testing", file.Dataset.ColumnNames))
		_, err = db.Exec(Insertrecords("testing", file.Dataset.ColumnNames), new...)
		if err != nil {
			panic(err)
		}
	}

	tx.Commit()

	//fmt.Println(Createtable(table, s))
	//fmt.Println(Insertrecords(table, s))
}
