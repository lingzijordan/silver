package main

import (
	"os"
	"bufio"
	"github.com/yewno/log"
	"strings"
	"fmt"
	"strconv"
	"math"
)

type Row struct {
	Yid string
	Cid string
	Score string
}

func main() {
	key := "/Users/ziling/Desktop/reverse_yids.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	reverseYid := make(map[string]string)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")
		reverseYid[arr[0]] = arr[1]
	}

	key = "/Users/ziling/Desktop/yid_cid_score.txt"
	f, err = os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner = bufio.NewScanner(f)
	maxScoreMap := make(map[string]float64)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		cid := arr[1]
		score, _ := strconv.ParseFloat(arr[2], 64)

		leadingYid, ok := reverseYid[yid]
		if !ok {
			log.Infof("%s doesn't exist!", yid)
			continue
		}

		yidCid := fmt.Sprintf("%s-%s", leadingYid, cid)

		existingScore, ok := maxScoreMap[yidCid]
		if !ok {
			maxScoreMap[yidCid] = score
		} else {
			maxScoreMap[yidCid] = math.Max(score, existingScore)
		}
	}

	flagFile := "/Users/ziling/Desktop/yid_cids_maxScore.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for yidCid, score := range maxScoreMap {

		arr := strings.Split(yidCid, "-")
		yid := arr[0]
		cid := arr[1]

		line := fmt.Sprintf("%s,%s,%s\n", yid, cid, strconv.FormatFloat(score, 'f', 12, 64))

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