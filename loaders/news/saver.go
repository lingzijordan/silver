package news

import (
	"sync"
	"regexp"
	"fmt"
	"strconv"
	"github.com/yewno/silver/services"
	"strings"
	"unicode/utf8"
	"os"
	"github.com/yewno/silver/formats"
	"github.com/yewno/log"
	"encoding/json"
)

func GetMetaReady(v *formats.NewsContentMeta) {
	var keywords string
	if v.FullText != "" {
		i := strings.Index(v.FullText, "Keywords:")
		if i != -1 {
			index := utf8.RuneCountInString(v.FullText[:i])
			s := v.FullText[index:]
			words := strings.Split(s, ":")[1]
			keywords = strings.TrimSpace(words)
		}

	}
	v.Keywords = keywords
}

func SaveOnS3(filepath string, sctx *services.ServiceContext, bytes []byte, key string) error {
	newsFile, err := os.OpenFile(filepath, os.O_RDWR | os.O_CREATE, 0644)
	if err != nil {
		log.WithError(err).Error("a")
		return err
	}
	if _, err := newsFile.Write(bytes); err != nil {
		log.WithError(err).Error("b")
		return err
	}
	if _, err := newsFile.Seek(0, 0); err != nil {
		log.WithError(err).Error("c")
		return err
	}

	newsObj := services.NewObject(newsFile, sctx.Cfg.Bucket, key, int64(len(bytes)))
	if err := sctx.Storage.Save(newsObj); err != nil {
		log.WithError(err).Error("")
		return err
	}
	newsObj.Close()

	return nil
}

func CacheNewsToS3(sctx *services.ServiceContext, newsMap map[string]*formats.NewsContentMeta, tmpdir string) ([]*formats.Pair, error) {
	var pairs []*formats.Pair

	sourceInChan := make(chan *formats.NewsContentMeta, len(newsMap))
	outChan := make(chan *formats.Pair, len(newsMap))

	for _, v := range newsMap {
		sourceInChan <- v
	}
	close(sourceInChan)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int, sctx *services.ServiceContext, inChan chan *formats.NewsContentMeta, outChan chan *formats.Pair, wg *sync.WaitGroup) {
			defer wg.Done()
			for v := range inChan {
				GetMetaReady(v)

				re := regexp.MustCompile(`(?m)[\r\n]+^.*(\w[-._\w]*\w@\w[-._\w]*\w\.\w{2,3}).*$`)
				res := re.ReplaceAllString(v.FullText, "")
				re2 := regexp.MustCompile("(?m)[\r\n]+^.*Reporting By|Editing by|Reuters Messaging|Page Editor.*$")
				res = re2.ReplaceAllString(res, "")
				//fmt.Println(res)

				news := res
				filepath := fmt.Sprintf("%s/%s", tmpdir, v.YId)
				newsKey := fmt.Sprintf("news/%s/%s/%s.txt", strconv.Itoa(v.Year), strconv.Itoa(v.Month), v.YId)
				metaKey := fmt.Sprintf("news/%s/%s/%s.json", strconv.Itoa(v.Year), strconv.Itoa(v.Month), v.YId)
				//log.Infof("file %s", filepath)

				byteArr := []byte(news)
				err := SaveOnS3(filepath, sctx, byteArr, newsKey)
				if err != nil {
					log.WithError(err)
				}

				byteArr, err = json.Marshal(v)
				err = SaveOnS3(filepath, sctx, byteArr, metaKey)
				if err != nil {
					log.WithError(err)
				}
				pair := &formats.Pair{
					Meta: metaKey,
					Content: newsKey,
				}
				outChan <- pair
			}
			log.Infof("finished worker %d", id)
		}(i, sctx, sourceInChan, outChan, &wg)
	}
	wg.Wait()
	close(outChan)

	for pair := range outChan {
		pairs = append(pairs, pair)
	}

	return pairs, nil
}
