package main

import (
	"os"
	"bufio"
	"strings"
	"github.com/yewno/log"
	"fmt"
)

func main() {

	key := "/Users/ziling/Desktop/yid_containerIds.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	containerIdYidsMap := make(map[string][]string)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		containerId := arr[1]

		yidArr, ok := containerIdYidsMap[containerId]
		if !ok {
			containerIdYidsMap[containerId] = []string{yid}
		} else {
			yidArr = append(yidArr, yid)
			containerIdYidsMap[containerId] = yidArr
		}
	}

	booksMap := make(map[string][]string) //containerId -> yids
	for k, v := range containerIdYidsMap {
		if len(v) > 1 {
			booksMap[k] = v
		}
	}

	yidCidMap := make(map[string][]string) //yid -> cids
	key = "/Users/ziling/Desktop/yid_cids.txt"
	f, err = os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner = bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		cids := arr[1]
		cidsArr := strings.Split(cids, " ")

		yidCidMap[yid] = cidsArr
	}

	yidCidFullMap := make(map[string][]string)
	for _, v := range booksMap {
		var cidsAll []string
		for _, yid := range v {
			cids, ok := yidCidMap[yid]
			if !ok {
				log.Infof(yid)
				continue
			}
			cidsAll = append(cidsAll, cids...)
		}

		for _, yid := range v {
			yidCidFullMap[yid] = cidsAll
		}
	}

	flagFile := "/Users/ziling/Desktop/yid_allcids.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for k, v := range yidCidFullMap {
		line := fmt.Sprintf("%s,%s\n", k, strings.Join(v, " "))

		log.Infof(line)

		_, err := file.WriteString(line)
		if err != nil {
			log.WithError(err)
			continue
		}
	}

	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}


}
