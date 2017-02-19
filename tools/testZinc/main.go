package main

import (
	"encoding/json"
	"os"
	"github.com/yewno/log"
)

type IdsList struct {
	Ids []string `json:"ids" form:"ids" binding:"required"`
}

func main() {
	test := &IdsList {
		Ids: []string{"123", "345", "567"},
	}

	result, _ := json.Marshal(test)

	flagFile := "/Users/ziling/testingResult.txt"
	file, err := os.OpenFile(flagFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	_, err = file.WriteString(string(result))
	if err != nil {
		log.WithError(err)
	}


	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}
}
