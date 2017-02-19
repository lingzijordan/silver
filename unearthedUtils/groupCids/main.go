package main

import (
	"os"
	"bufio"
	"strings"
	"fmt"
	"github.com/yewno/log"
)

func main() {
	key := "/Users/ziling/Desktop/yid_cid_score.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	yidCids := make(map[string][]string)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		cid := arr[1]

		cids, ok := yidCids[yid]
		if !ok {
			newCids := []string{cid}
			yidCids[yid] = newCids
		} else {
			cids = append(cids, cid)
			yidCids[yid] = cids
		}
	}

	flagFile := "/Users/ziling/Desktop/yid_cids.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for yid, cids := range yidCids {

		str := strings.Join(cids, " ")

		line := fmt.Sprintf("%s,%s\n", yid, str)

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
