package main

import (
	"os"
	"bufio"
	"strings"
	"github.com/yewno/log"
	"fmt"

)

type YidChapter struct {
	Yid string
	Chapter int
}

type Row struct {
	Yid string
	Cid string
	Score string
}

func main() {

	//key := "/Users/ziling/Desktop/associated_yids.txt"
	//f, err := os.Open(key)
	//if err != nil {
	//	log.WithError(err)
	//}
	//defer f.Close()
	//
	//ycScoreMap := make(map[string]float64)
	//
	//scanner := bufio.NewScanner(f)
	//for scanner.Scan() {
	//	line := scanner.Text()
	//	arr := strings.Split(line, ",")
	//
	//	yid := arr[0]
	//	cid := arr[1]
	//	yidCid := fmt.Sprintf("%s-%s", yid, cid)
	//	score, _ := strconv.ParseFloat(arr[2], 64)
	//
	//	existingScore, ok := ycScoreMap[yidCid]
	//	if !ok {
	//		ycScoreMap[yidCid] = score
	//	} else {
	//		ycScoreMap[yidCid] = math.Max(score, existingScore)
	//	}
	//}
	//
	//flagFile := "/Users/ziling/Desktop/yid_cid_maxScore.txt"
	//file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
	//if err != nil {
	//	log.WithError(err)
	//}
	//defer file.Close()
	//
	//for k, score := range ycScoreMap {
	//
	//	arr := strings.Split(k, "-")
	//	yid := arr[0]
	//	cid := arr[1]
	//
	//	line := fmt.Sprintf("%s,%s,%s\n", yid, cid, strconv.FormatFloat(score, 'f', 12, 64))
	//
	//	log.Infof(line)
	//
	//	_, err := file.WriteString(line)
	//	if err != nil {
	//		log.WithError(err)
	//		continue
	//	}
	//}
	//
	//if _, err := file.Seek(0, 0); err != nil {
	//	log.WithError(err).Error("")
	//}

	key := "/Users/ziling/Desktop/yid_cids_maxScore.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	ycScoreMap := make(map[string][]string)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		cid := arr[1]

		var newCids []string
		cids, ok := ycScoreMap[yid]
		if !ok {
			newCids = append(newCids, cid)
			ycScoreMap[yid] = newCids
		} else {
			cids = append(cids, cid)
			ycScoreMap[yid] = cids
		}
	}

	flagFile := "/Users/ziling/Desktop/yid_cids_finalized.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for k, cids := range ycScoreMap {

        str := strings.Join(cids, " ")

		line := fmt.Sprintf("%s,%s\n", k, str)

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