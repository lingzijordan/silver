package companySegment

import (
	"database/sql"
	"io/ioutil"
	"os"
	"strings"
	"github.com/yewno/log"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/formats"
	"bufio"
	"fmt"
	"sync"
)

func Process(sctx *services.ServiceContext, key string) error {

	tmpdir, err := ioutil.TempDir("/tmp", sctx.Cfg.Source)
	if err != nil {
		log.WithError(err).Error("unable to make tmp dir")
		return err
	}
	defer os.RemoveAll(tmpdir)

	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return err
	}
	defer object.Close()

	//file, _:= os.Open("/Users/ziling/Desktop/testing")

	sourceInChan := make(chan *formats.CompanySegment, 4294967295)

	scanner := bufio.NewScanner(object.File)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")

		record := &formats.CompanySegment{
			CompanyName:            arr[0],
			CountryCodeIncorp:      arr[1],
			GICS:                   arr[2],
			LatestDate:             arr[3],
			Template:               arr[4],
			ConsCode:               arr[5],
			OperatingRev:           arr[6],
			NumEmployee:            arr[7],
			Indep:                  arr[8],
			ISIN:                   arr[9],
			Date:                   arr[10],
			Label:                  arr[11],
			Sales:                  arr[12],
			Profit:                 arr[13],
			Assets:                 arr[14],
			Depreciation:           arr[15],
			Ppe:                    arr[16],
			Randd:                  arr[17],
			CapitalExpenditure:     arr[18],
			GrossPremiumthusd:      arr[19],
			GrossPremium:           arr[20],
			NetPremiumthusd:        arr[21],
			NetPremium:             arr[22],
			Unit:                   arr[23],
			Currency:               arr[24],
		}
		sourceInChan <- record

		fmt.Println(record)

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err.Error())
	}

	tx, err := sctx.DB.Begin()

	_, err = sctx.DB.Exec(CreateTable(sctx.Cfg.DBtable), )
	if err != nil {
		log.Debugf(err.Error())
	}

	close(sourceInChan)

	log.Infof("sourceInChan is %d", len(sourceInChan))

	var w sync.WaitGroup
	for i := 0; i < 50; i++ {
		w.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.CompanySegment, table string, w *sync.WaitGroup) {
			defer w.Done()
			for record := range inChan {

				err = LoadData(db, record, table)
				if err != nil {
					log.Debugf(err.Error())
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