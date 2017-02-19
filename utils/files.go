package utils

import (
	"os"
	"io/ioutil"
)

func BytesToFile(body []byte) (*os.File, int64, error) {

	temp, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, -1, err
	}

	size, err := temp.Write(body)
	_, err = temp.Seek(0, 0)
	return temp, int64(size), err
}
