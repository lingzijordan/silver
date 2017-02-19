package main

import (
	"os"
	"github.com/yewno/log"
	"github.com/yewno/log/handlers/text"
	"fmt"
)

func main() {
	log.SetHandler(text.Default)
	_, err := os.Stat("/Users/ziling/Documents/BSB/b3kat_export_2016_11_teil01.marc.xml")
	if err != nil {
		log.WithError(err).Error("")
	}

	fmt.Println("file exists")
}
