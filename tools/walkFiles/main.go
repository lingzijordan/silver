package main

import (
"fmt"
"io/ioutil"
	"github.com/yewno/log"
	"strings"
	"os"
	"bufio"
)

func getFiles() []string {
	var keys []string
	files, err := ioutil.ReadDir("/Users/ziling/Documents/NYSE/")
	if err != nil {
		log.Debugf(err.Error())
		return keys
	}

	files2, err := ioutil.ReadDir("/Users/ziling/Documents/NASDAQ/")
	if err != nil {
		log.Debugf(err.Error())
		return keys
	}

	for _, file := range files {
		key := fmt.Sprintf("/Users/ziling/Documents/NYSE/%s", file.Name())
		keys = append(keys, key)
	}
	for _, file2 := range files2 {
		key2 := fmt.Sprintf("/Users/ziling/Documents/NASDAQ/%s", file2.Name())
		keys = append(keys, key2)
	}
	fmt.Println("%d", len(keys))
	return keys
}

func main() {
	keys := getFiles()
	keyString := strings.Join(keys, ",")

	key := "/Users/ziling/Desktop/testing3"

	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()

		if !strings.Contains(keyString, line) {
			fmt.Println("%s", line)
		}
	}

}
