package main

import (
	"os"
	"bufio"
	"strings"
	"fmt"
	"github.com/yewno/log"
)

func main() {
	key := "/Users/ziling/secMeta.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	flagFile := "/Users/ziling/yids.txt"
	file, err := os.OpenFile(flagFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		fname := arr[1]
		fname = strings.Split(fname, "/")[1]
		arr = strings.Split(fname, "-")
		year := arr[1]
		month := arr[2]
		day := arr[3]

		line2 := fmt.Sprintf("%s,%s,%s,%s\n", year, month, day, yid)

		_, err := file.WriteString(line2)
		if err != nil {
			log.WithError(err)
			continue
		}

	}

	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}
}