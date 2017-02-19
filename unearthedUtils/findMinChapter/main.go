package main

import (
	"os"
	"bufio"
	"strings"
	"github.com/yewno/log"
	"fmt"
	"strconv"
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

	key := "/Users/ziling/Desktop/yid_containerIds.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	containerIdYidsMap := make(map[string][]*YidChapter)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		containerId := arr[1]
		chapter, _ := strconv.Atoi(arr[2])

		yidArr, ok := containerIdYidsMap[containerId]
		entry := &YidChapter{
			Yid: yid,
			Chapter: chapter,
		}
		if !ok {
			a := []*YidChapter{}
			a = append(a, entry)
			containerIdYidsMap[containerId] = a
		} else {
			yidArr = append(yidArr, entry)
			containerIdYidsMap[containerId] = yidArr
		}
	}

	println("%d", len(containerIdYidsMap))

	conChaMap := make(map[string]string)
	reverseMap := make(map[string]string)

	for k, v := range containerIdYidsMap {
		chapter := 100
		var yid string
		for _, obj := range v {
			if chapter > obj.Chapter {
				chapter = obj.Chapter
				yid = obj.Yid
			}
		}
		conChaMap[k] = yid
	}

	leadingYidMap := make(map[string][]string)
	for k, v := range containerIdYidsMap {
		leadingYid := conChaMap[k]
		var yidArr []string
		for _, yid := range v {
			yidArr = append(yidArr, yid.Yid)
			reverseMap[yid.Yid] = leadingYid
		}
		leadingYidMap[leadingYid] = yidArr
	}

	key = "/Users/ziling/Desktop/yid_cid_score.txt"
	f, err = os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner = bufio.NewScanner(f)
	var rows []*Row

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")
		yid := arr[0]
		cid := arr[1]
		score := arr[2]

		leadingYid, ok := reverseMap[yid]
		if !ok {
			continue
		}

		yid = leadingYid
		row := &Row{
			Yid: yid,
			Cid: cid,
			Score: score,
		}

		rows = append(rows, row)
	}



	flagFile := "/Users/ziling/Desktop/associated_yids.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for _, v := range rows {
		line := fmt.Sprintf("%s,%s,%s\n", v.Yid, v.Cid, v.Score)

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