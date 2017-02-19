package main

import (
	"os"
	"bufio"
	"strings"
	"github.com/yewno/log"
	"sort"
	"strconv"
	"fmt"
)

type Row struct {
	Yid string
	Cid string
	Score string
}

type Concept struct {
	Cid string
	Score string
}

type Concepts []Concept

func (concepts Concepts) Len() int {
	return len(concepts)
}

func (concepts Concepts) Less(i, j int) bool {
	return concepts[i].Score < concepts[j].Score;
}

func (concepts Concepts) Swap(i, j int) {
	concepts[i], concepts[j] = concepts[j], concepts[i]
}

func main() {
	key := "/Users/ziling/Desktop/yid_cids_maxScore.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	conceptMap := make(map[string]Concepts)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		concept := Concept{
			Cid: arr[1],
			Score: arr[2],
		}

		concepts, ok := conceptMap[yid]
		if !ok {
			a := Concepts{concept}
			conceptMap[yid] = a
		} else {
			concepts = append(concepts, concept)
			conceptMap[yid] = concepts
		}
	}

	println(len(conceptMap))

	for yid, concepts := range conceptMap {
		sort.Sort(concepts)

		size := len(concepts)
		if size > 50 {
			concepts = concepts[size-50:size]
		}

		var newConcepts Concepts
		for _, concept := range concepts {
			score, _ := strconv.ParseFloat(concept.Score, 64)
			if score > 0.7 {
				newConcepts = append(newConcepts, concept)
			}
		}

		if len(newConcepts) == 0 && len(concepts) < 10{
			newConcepts = concepts
		} else if len(newConcepts) == 0 {
			newConcepts = concepts[len(concepts)-10:]
		}

		conceptMap[yid] = newConcepts
	}

	flagFile := "/Users/ziling/Desktop/yid_cids_books.txt"
	file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for yid, concepts := range conceptMap {
		conceptArr := []string{}
		for _, concept := range concepts {
			conceptArr = append(conceptArr, concept.Cid)
		}

		str := strings.Join(conceptArr, " ")

		line := fmt.Sprintf("%s,%s\n", yid, str)

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
