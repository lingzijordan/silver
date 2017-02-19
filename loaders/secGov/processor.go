package secGov

import (
	"io/ioutil"
	"os"
	"github.com/yewno/silver/services"
	"github.com/yewno/log"
	"compress/gzip"
	//"github.com/yewno/silver/utils"
	"github.com/yewno/silver/utils"
	"path"
	"fmt"
	"strings"
	"crypto/sha1"
	"io"
)

func Process(sctx *services.ServiceContext, key string) (string, error) {

	tmpdir, err := ioutil.TempDir("/tmp", sctx.Cfg.Source)
	if err != nil {
		log.WithError(err).Error("unable to make tmp dir")
		return "", err
	}
	defer os.RemoveAll(tmpdir)

	object := services.NewObject(nil, sctx.Cfg.Bucket, key, 10)
	if err := sctx.Storage.Get(object); err != nil {
		log.WithError(err).Error("")
		return "", err
	}
	defer object.Close()

	reader, err := gzip.NewReader(object.File)
	if err != nil {
		log.WithError(err).Error("")
		log.Debugf(key)
		return "", err
	}

	bytesArr, err := ioutil.ReadAll(reader)
	if err != nil {
		log.WithError(err).Error("")
		return "", err
	}
	reader.Close()

	base := path.Base(key)
	fname := strings.Split(base, ".")[0]

	arr := strings.Split(fname, "-")
	//cik := arr[0]
	//year := arr[1]
	//month := arr[2]
	//day := arr[3]
	hashcode := arr[4]

	hash := sha1.New()
	hashString := fmt.Sprintf("%s%s", "sec-gov", hashcode)
	io.WriteString(hash, hashString)
	yid := fmt.Sprintf("%x", hash.Sum(nil))[0:32]

	returnString := fmt.Sprintf("%s,%s\n", yid, key)

	key = fmt.Sprintf("sec-gov/%s.txt", yid)

	utils.SavetoS3CustomKey(bytesArr, "yewno-finance", key, sctx)

	return returnString, nil
}