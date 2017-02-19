package main

import (
	"bufio"
	"github.com/yewno/log"
	"strings"
	//"github.com/yewno/silver/utils"
	"github.com/yewno/silver/services"
	"github.com/yewno/silver/config"
	"errors"
	//"time"
	"fmt"
	"github.com/yewno/log/handlers/text"
	"compress/gzip"
	"sync"
	"os"
)

type Info struct {
	Company1 string
	Company2 string
	Percentage string
}

func main() {
	log.SetHandler(text.Default)
	log.SetLevel(log.DebugLevel)

	cfg := &config.Config{
		Bucket:          "yewno-content-crawled",
		Credentials:     services.NewCredentials(),
		Region:          "us-west-2",
		ConfigTbl:       "silverDBConfig",
		StatsTbl:        "",
		DBtable:         "",
		Source:          "sec-gov",
		DBType:          "postgres",
		ProcessedQueue:  "",
	}

	dbcred := &config.DBcredentials{
		Database:        "finance_testing",
	}
	err := services.GetDBCredentials(cfg, dbcred)
	if err != nil {
		log.Debugf(err.Error())
	}

	if dbcred.Ip == "localhost" {
		dbcred.Ip = ""
	}

	sctx, err := services.NewServiceContext(cfg, dbcred)
	if err != nil {
		log.Debugf(err.Error())
	}
	//defer sctx.DB.Close()


	//keys := []string{"/Users/ziling/Documents/sec/0000847383-2008-07-25-3ba871f4c64e71575e8e045c965d7e20.txt"}
	var counter int

	sourceInChan := make(chan string, 306215)
	key := "/Users/ziling/k.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		sourceInChan <- line
	}
	close(sourceInChan)

	sourceOutChan := make(chan string, len(sourceInChan))
	goodFiles := make(chan string, 53195)

	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(id int, inChan, outChan, keyChan chan string, wg *sync.WaitGroup) {
			defer wg.Done()
			for k := range inChan {
				if strings.Contains(k, ".txt") {
					//percentage, err := capturePercentage(sctx, k)
					//company, err := captureCompany1(sctx, k)
					section1, section2, section3, err := captureItems(sctx, k)
					if err != nil {
						log.WithError(err)
						continue
					}
					//log.Infof("%s : %s", k, item)
					if section1 == "" || section2 == "" || section3 == ""{
						log.Infof("Empty sections %s", k)
						outChan <- k
					} else {
						keyChan <- k
					}

					counter++
				}
			}
			//log.Infof("finished worker %d", id)
		}(i, sourceInChan, sourceOutChan, goodFiles, &wg)
	}
	wg.Wait()
	close(sourceOutChan)
	close(goodFiles)

	fmt.Println("%d", len(sourceOutChan))
	fmt.Println("%d", len(goodFiles))

	flagFile := "/Users/ziling/badItem2.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for k := range sourceOutChan {
		s := fmt.Sprintf("%s\n", k)
		_, err := file.WriteString(s)
		if err != nil {
			log.WithError(err)
			continue
		}
	}

	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}

	//for _, key := range keys {
	//	if strings.Contains(key, ".txt") {
	//		counter++
	//		percentage, err := capturePercentage(sctx, key)
	//		if err != nil {
	//			log.WithError(err)
	//			continue
	//		}
	//		log.Infof("%s : %s", key, percentage)
	//	} else {
	//		continue
	//	}
	//}

}

func hasPrefix1(line string) bool {

	item1Names := []string{"item 1", "item 1(a)", "item 1.", "item 1(a)."}

	for _, word := range item1Names {
		if strings.HasPrefix(line, word) {
			return true
		}
	}

	return false
}

func hasPrefix2(line string) bool {
	item2Names := []string{"item 2", "item 2(a)", "item 2.", "items 2(a)-2(c)."}

	for _, word := range item2Names {
		if strings.HasPrefix(line, word) {
			return true
		}
	}

	return false

}

func captureItems(sctx *services.ServiceContext, key string) (string, string, string, error) {
	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return "", "", "", err
	}
	defer object.Close()

	reader, err := gzip.NewReader(object.File) //object.File
	if err != nil {
		log.WithError(err).Error("")
		return "", "", "", err
	}

	textMap := make(map[int]string)

	var index1, index2, index3, index4, index5 int
	var item1, item2, item4 []string

	scanner := bufio.NewScanner(reader)
	var counter int



	for scanner.Scan() {
		line := scanner.Text()

		newLine := strings.TrimSpace(strings.ToLower(line))

		if strings.Contains(line, "<TYPE>") {
			if !strings.Contains(line, "13G") {
				return "", "", "", errors.New("The file is not 13G!")
			}
		}

		if strings.TrimSpace(line) == "" || strings.Contains(line, "---------------") || strings.Contains(line, "______________________") || strings.Contains(line, " - - - - - - - - - - - - - - - - - - -"){continue}

		if hasPrefix1(newLine) {
			index1 = counter
		}
		if hasPrefix2(newLine) {
			index2 = counter
		}
		if strings.HasPrefix(newLine, "item 3") {
			index3 = counter
		}
		if strings.HasPrefix(newLine, "item 4") {
			index4 = counter
		}
		if strings.HasPrefix(newLine, "item 5") {
			index5 = counter
		}

		textMap[counter] = line

		//fmt.Println(strconv.Itoa(counter))
		//fmt.Println(line)
		counter++
	}

	for index := index1; index < index2; index++ {
		item1 = append(item1, textMap[index])
	}
	for index := index2; index < index3; index++ {
		item2 = append(item2, textMap[index])
	}
	for index := index4; index < index5; index++ {
		item4 = append(item4, textMap[index])
	}

	section1 := strings.Join(item1, "\n")
	section2 := strings.Join(item2, "\n")
	section3 := strings.Join(item4, "\n")

	fmt.Println(section1)
	fmt.Println("*******************")
	fmt.Println(section2)
	fmt.Println("*******************")
	fmt.Println(section3)

	return section1, section2, section3, nil

}


func captureCompany1(sctx *services.ServiceContext, key string) (string, error) {
	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return "", err
	}
	defer object.Close()

	reader, err := gzip.NewReader(object.File) //object.File
	if err != nil {
		log.WithError(err).Error("")
		return "", err
	}

	textMap := make(map[int]string)

	var company string

	scanner := bufio.NewScanner(reader)
	var counter int
	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, "<TYPE>") {
			if !strings.Contains(line, "13G") {
				return "", errors.New("The file is not 13G!")
			}
		}

		if strings.TrimSpace(line) == "" || strings.Contains(line, "---------------") || strings.Contains(line, "______________________") || strings.Contains(line, " - - - - - - - - - - - - - - - - - - -"){continue}

		textMap[counter] = line

		//fmt.Println(strconv.Itoa(counter))
		//fmt.Println(line)
		counter++
	}

	for line, str := range textMap {
		if strings.Contains(strings.ToUpper(str), "NAME OF ISSUER") {
			trimmed := strings.TrimSpace(str)
			if len(trimmed) > len("NAME OF ISSUER") + 5 {
				company = trimmed
			} else {
				prevStr := textMap[line-1]
				company = prevStr
			}
		}
	}



	return strings.TrimSpace(company), nil
}

func capturePercentage(sctx *services.ServiceContext, key string) (string, error) {

	//key = "/Users/ziling/Documents/sec/0000847383-2008-07-25-3ba871f4c64e71575e8e045c965d7e20.txt"

	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return "", err
	}
	defer object.Close()

	reader, err := gzip.NewReader(object.File) //object.File
	if err != nil {
		log.WithError(err).Error("")
		return "", err
	}

	textMap := make(map[int]string)

	var percentage string

	//f, err := os.Open(key)
	//if err != nil {
	//	log.WithError(err)
	//}
	//defer f.Close()

	scanner := bufio.NewScanner(reader)
	var counter int
	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, "<TYPE>") {
			if !strings.Contains(line, "13G") {
				return "", errors.New("The file is not 13G!")
			}
		}

		if strings.TrimSpace(line) == "" || strings.Contains(line, "---------------") {continue}

		textMap[counter] = line

		//fmt.Println(strconv.Itoa(counter))
		//fmt.Println(line)
		counter++
	}
	numbers := "0123456789."

	for line, str := range textMap {
		if str == "%" {continue}

		if strings.Contains(strings.ToLower(str), "percent of class") {
			if strings.Contains(str, "%") {
				percentage = str
				break
			} else if strings.Contains(strings.ToLower(textMap[line+1]), "percent of class") {
				if strings.Contains(str, "%") {
					newLine := line + 1
					percentage = textMap[newLine]
					break
				}
			} else if strings.Contains(strings.ToLower(textMap[line+2]), "percent of class") {
				if strings.Contains(str, "%") {
					newLine := line + 2
					percentage = textMap[newLine]
					break
				}
			}
			continue
		} else if strings.Contains(strings.ToLower(textMap[line]), "%"){
			percentage = textMap[line]
			break
		}
	}


	//fmt.Println(percentage)

	if percentage == "" {
		return "", nil
	}

	perc := strings.Index(percentage, "%")
	num := 0
	for first:=perc-1; first > -1; first-- {
		if !strings.Contains(numbers, string(percentage[first])) {
			num = first+1
			break
		}
	}

	percentage = percentage[num : perc+1]
	//if string(percentage[0]) == "." {
	//	percentage = fmt.Sprintf("0%s", percentage)
	//}

	//fmt.Println(percentage)

	return strings.TrimSpace(percentage), nil

}
