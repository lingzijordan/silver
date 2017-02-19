package companyIndustry

import (
	"strings"
	"os"
	"bufio"
	"fmt"
	//"sync"
	//"database/sql"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"github.com/yewno/silver/formats"
	//"github.com/yewno/cobalt-50/files"
	"sync"
	"database/sql"
)

func lookUp() map[string]string {
	table := map[string]string{}

	key := "/Users/ziling/Desktop/testing2"

	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")

		table[arr[0]] = arr[1]
	}

	return table
}

func Process(sctx *services.ServiceContext, key string) error {
	table := lookUp()

	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	sourceInChan := make(chan *formats.CompanyIndustry, 60000)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")

		if arr[0] == "null" || arr[1] == "null" {continue}

		digits := len(arr[1])
		cp1 := arr[1][:digits-1]
		cp2 := arr[1][:digits-2]

		section, ok := table[cp2]
		if !ok {
			section = "null"
		}

		entry := &formats.CompanyIndustry{
			ISIN:    arr[0],
			NACE:    arr[1],
			CP1:     cp1,
			CP2:     cp2,
			Section: section,
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
		go func(id int, db *sql.DB, inChan chan *formats.CompanyIndustry, table string, wg *sync.WaitGroup) {
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