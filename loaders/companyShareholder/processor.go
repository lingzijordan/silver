package companyShareholder

import (
	"fmt"
	"database/sql"
	"io/ioutil"
	"os"
	"bufio"
	"strings"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	"github.com/yewno/silver/services"
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

	sourceInChan := make(chan *formats.Shareholders, 4294967295)

	scanner := bufio.NewScanner(object.File)
	var counter int
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")

		if len(arr) < 34 {
			counter++
			fmt.Println("%v", arr)
			fmt.Println(len(arr))
			continue
		}

		record := &formats.Shareholders{
			CompanyName:                      arr[0],
			CountryCode:                      arr[1],
			GICSCode:                         arr[2],
			LatestAccountDate:                arr[3],
			Template:                         arr[4],
			ConsCode:                         arr[5],
			Operating:                        arr[6],
			NumberOfEmployee:                 arr[7],
			IndepInd:                         arr[8],
			ISIN:                             arr[9],
			BvD:                              arr[10],
			Comments:                         arr[11],
			RecordedShareholders:             arr[12],
			Name:                             arr[13],
			Salutation:                       arr[14],
			FirstName:                        arr[15],
			LastName:                         arr[16],
			BvDIDNumber:                      arr[17],
			TickerSymbol:                     arr[18],
			CountryISOCode:                   arr[19],
			City:                             arr[20],
			Type:                             arr[21],
			NACECoreCode:                     arr[22],
			NACEText:                         arr[23],
			NAICS2012CoreCode:                arr[24],
			NAICS2012Text:                    arr[25],
			Direct:                           arr[26],
			Total:                            arr[27],
			InformationOnPossibleChange:      arr[28],
			InformationSource:                arr[29],
			InformationDate:                  arr[30],
			OperatingRevenue:                 arr[31],
			TotalAssets:                      arr[32],
			NoOfEmployees:                    arr[33],
		}
		sourceInChan <- record

		//fmt.Println(record)

	}
	fmt.Println("count is shorter: ")
	fmt.Println(counter)

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
	counter = 0
	var w sync.WaitGroup
	for i := 0; i < 50; i++ {
		w.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.Shareholders, table string, w *sync.WaitGroup) {
			defer w.Done()
			for record := range inChan {

				err = LoadData(db, record, table)
				if err != nil {
					counter++
					log.Debugf(err.Error())
				}
			}
			log.Infof("finished worker %d", id)
		}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &w)
	}
	w.Wait()

	tx.Commit()

	fmt.Println(counter)

	return nil
}