package companyStandard

import (
	"os"
	"bufio"
	"strings"
	"fmt"
	"sync"
	"database/sql"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
)

func Process(sctx *services.ServiceContext, key string) error {

	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	sourceInChan := make(chan *formats.CompanyStandard, 60000)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")

		entry := &formats.CompanyStandard{
			CompanyName:    arr[0],
			Isin:           arr[1],
			Gics:           arr[2],
			GicsDesc:       arr[3],
			Naics:          arr[4],
			NaicsDesc:      arr[5],
			USSic:          arr[6],
			USSicDesc:      arr[7],
			Nace:           arr[8],
			NaceDesc:       arr[9],
		}

		sourceInChan <- entry
		fmt.Println("%v", entry)
	}
	close(sourceInChan)

	fmt.Println("%d", len(sourceInChan))

	tx, err := sctx.DB.Begin()
	if err != nil {
		log.Debugf(err.Error())
	}

	_, err = sctx.DB.Exec(CreateTable(sctx.Cfg.DBtable), )
	if err != nil {
		log.Debugf(err.Error())
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int, db *sql.DB, inChan chan *formats.CompanyStandard, table string, wg *sync.WaitGroup) {
			defer wg.Done()
			for record := range inChan {

				err = LoadData(db, record, table)
				if err != nil {
					log.WithError(err)
				}
			}
			//log.Infof("finished worker %d", id)
		}(i, sctx.DB, sourceInChan, sctx.Cfg.DBtable, &wg)
	}
	wg.Wait()

	tx.Commit()

	return nil
}