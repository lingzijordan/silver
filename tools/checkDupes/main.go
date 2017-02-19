package main

import (
	"os"
	"bufio"
	"strings"
	"github.com/yewno/log"
	"fmt"
)

func main() {
	key := "/Users/ziling/secMeta.txt"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	countMap := make(map[string]int)

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, ",")

		yid := arr[0]
		count, ok := countMap[yid]
		if !ok {
			countMap[yid] = 1
		} else {
			countMap[yid] = count + 1
		}
	}

	var num int
	for k, v := range countMap {
		if v > 1 {
			num++
			fmt.Println("%s %d", k, v)
		}
	}

	fmt.Println(num)
}
