package companyCustomer

import (
	"database/sql"
	"os"
	"strings"
	"sync"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	"github.com/yewno/silver/services"
	"bufio"
	//"io/ioutil"
	"bytes"
)

func Process(sctx *services.ServiceContext, key string) error {

	//tmpdir, err := ioutil.TempDir("/tmp", sctx.Cfg.Source)
	//if err != nil {
	//	log.WithError(err).Error("unable to make tmp dir")
	//	return err
	//}
	//defer os.RemoveAll(tmpdir)
	//
	//object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	//if err := sctx.Storage.Get(object); err != nil {
	//	log.WithError(err).Error("")
	//	return err
	//}
	//defer object.Close()
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	tx, err := sctx.DB.Begin()
	if err != nil {
		log.WithError(err)
	}

	_, err = sctx.DB.Exec(CreateTable(sctx.Cfg.DBtable), )
	if err != nil {
		log.WithError(err)
	}

	//file, _:= os.Open("/Users/ziling/Desktop/company_customer")

	sourceInChan := make(chan *formats.Customer, 90000)

	scanner := bufio.NewScanner(f)
	var counter int
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")

		var record *formats.Customer

		if len(arr) == 16 {
			counter++
			record = &formats.Customer{
				CompanyName:               strings.Replace(arr[0], "\"", "", -1),
				CountryCodeIncorp:         string(bytes.Trim([]byte(arr[1]), "\x00")),
				GICS:                      string(bytes.Trim([]byte(arr[2]), "\x00")),
				LatestAccountDate:         string(bytes.Trim([]byte(arr[3]), "\x00")),
				Template:                  string(bytes.Trim([]byte(arr[4]), "\x00")),
				ConsCode:                  string(bytes.Trim([]byte(arr[5]), "\x00")),
				OperatingRev:              string(bytes.Trim([]byte(arr[6]), "\x00")),
				NumberEmployee:            string(bytes.Trim([]byte(arr[7]), "\x00")),
				Indep:                     string(bytes.Trim([]byte(arr[8]), "\x00")),
				ISIN:                      string(bytes.Trim([]byte(arr[9]), "\x00")),
				MajorCustomersDate:        string(bytes.Trim([]byte(arr[10]), "\x00")),
				MajorCustomersName:        strings.Replace(arr[11], "\"", "", -1),
				MajorCustomersRevenue:     strings.Replace(arr[12], ",", "", -1),
				MajorCustomersRevenuePer:  string(bytes.Trim([]byte(arr[13]), "\x00")),
				MajorCustomersUnit:        string(bytes.Trim([]byte(arr[14]), "\x00")),
				MajorCustomersCurrency:    string(bytes.Trim([]byte(arr[15]), "\x00")),
			}

			sourceInChan <- record

		}

		if counter % 1000 == 0 {
			log.Infof("%d", counter)
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err.Error())
	}

	close(sourceInChan)

	log.Infof("sourceInChan is %d", len(sourceInChan))

	var w sync.WaitGroup
	for i := 0; i < 50; i++ {
		w.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.Customer, table string, w *sync.WaitGroup) {
			defer w.Done()
			for record := range inChan {

				err = LoadData(db, record, table)
				if err != nil {
					log.WithError(err)
				}
			}
			log.Infof("finished worker %d", id)
		}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &w)
	}
	w.Wait()
	tx.Commit()

	//}
	return nil
}