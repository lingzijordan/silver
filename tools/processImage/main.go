package main

import (
	"flag"
	"github.com/yewno/log"
	"github.com/yewno/log/handlers/text"
	caws "github.com/yewno/carbon/aws"
	"github.com/yewno/silver/config"
	"github.com/yewno/silver/services"
	"time"
	"github.com/yewno/silver/utils"
	"path"
	"strings"
	"io/ioutil"
	"compress/gzip"
	"encoding/xml"
	"crypto/sha1"
	"io"
	"fmt"
	"os"
	"os/exec"
	"errors"
)

type Article struct {
	XMLName  xml.Name `xml:"product"`
	ID       string  `xml:"a001"`
}

var (
	flagBucket string
	flagAWSRegion string
	flagSource string
	flagQueue   string
	flagTimeFrame time.Duration
)

const(
	source = "mitpress"
)

func init() {

	flag.StringVar(&flagBucket, "bucket", "yewno-content-new", "which bucket to save csv files")
	flag.StringVar(&flagAWSRegion, "region", "us-west-2", "region")
	flag.StringVar(&flagSource, "source", "mitpress", "data source to be ingested")
	flag.StringVar(&flagQueue, "processed-queue", "yewno-indexing-finance", "queue where pairs to sent for ingestion")
	flag.DurationVar(&flagTimeFrame, "duration", 365 * 24 * time.Hour, "how far back to search")

	log.SetHandler(text.Default)
	flag.Parse()
}

type Meta struct {
	Yid string `json:"yid"`
	Key string `json:"original,omitempty"`
	Key1x string `json:"small(100x100),omitempty"`
	Key2x string `json:"medium(200x200),omitempty"`
	Key3x string `json:"large(400x400),omitempty"`
	Base string `json:"base"`
	Bucket string `json:"bucket"`
	Source string `json:"source"`
	Type string `json:"type"`
	SourceId string `json:"sourceId"`
}

func setId(sourceId string) string {

	//log.Infof(sourceId)

	hash := sha1.New()
	io.WriteString(hash, source)
	io.WriteString(hash, sourceId)
	return fmt.Sprintf("%x", hash.Sum(nil))[0:32]
}

func UploadMetadata(newsMap map[string]*Meta) error {
	batch := caws.NewDynamoBatch()
	for _, v := range newsMap {

		if v.Yid == "" || v.Key == ""{
			continue
		}

		log.Debugf("uploading to dynamo table 'ImageMeta'")
		if err := batch.Add("mediaMeta", *v); err != nil {
			log.WithError(err)
			return err
		}
	}
	if err := batch.Flush(); err != nil {
		log.WithError(err)
		return err
	}
	return nil
}

func resizeImages(originalKey string, size string) (string, error) {
	baseFile := path.Base(originalKey)
	cId := strings.Split(baseFile, ".")[0]
	key := fmt.Sprintf("%s/%s@%s.jpg", "mitpress", cId, size)
	localKey := fmt.Sprintf("/tmp/%s.jpg", cId)
	caws.SetS3Config(map[string]string{"region": "us-west-2"})
	exists, err := caws.S3FileExists("yewno-content-new", key)
	if err != nil {
		log.WithError(err).Error("")
		return cId, err
	}
	if !exists {
		log.Infof("new file %s", cId)
		caws.SetS3Config(map[string]string{"region": "us-west-2"})
		s3File, err := caws.S3ToTempFile("yewno-content-new", originalKey)
		if err != nil {
			log.WithError(err).Error("")
			return key, err
		}
		defer os.Remove(s3File)

		status := make(chan string, 1)
		defer os.Remove(localKey)
		go func(closeChan chan string) {
			cli := "convert"
			// convert -define jpeg:size=500x180  hatching_orig.jpg  -auto-orient -thumbnail 250x90   -unsharp 0x.5  thumbnail.gif
			args := []string{s3File, "-thumbnail", size, "-quality", "80", "-unsharp", "0x.5", localKey}
			cmd := exec.Command(cli, args...)
			if err := cmd.Start(); err != nil {
				closeChan <- err.Error()
			}
			timer := time.AfterFunc(2*time.Second, func() {
				cmd.Process.Kill()
				log.Warn("convert killed...")
			})
			err = cmd.Wait()
			timer.Stop()
			if err != nil {
				closeChan <- err.Error()
			}
			closeChan <- ""
		}(status)
		if resp := <-status; resp != "" {
			err = errors.New(resp)
			return key, err
		}
		f, err := os.Open(localKey)
		if err != nil {
			log.WithError(err).Error("")
			return key, err
		}
		defer f.Close()
		caws.SetS3Config(map[string]string{"region": "us-west-2"})
		if err := caws.S3UploadWithContentType(f, "yewno-content-new", key, "image/jpeg"); err != nil {
			log.WithError(err).Error("")
			return key, err
		}
	}
	return key, nil
}

func Process(sctx *services.ServiceContext, keys []string) error {
	metaMap := map[string]*Meta{}

	for _, key := range keys {

		ext := path.Ext(key)
		base := path.Base(key)
		fname := strings.Split(base, ".")[0]
		switch ext {
		case ".jpg":

			if strings.Contains(key, "@") {
				continue
			}

			//resizedKey, err := resizeImages(key, "100x100")
			//if err != nil {
			//	log.WithError(err).Error("")
			//}
			//
			//resizedKey2, err := resizeImages(key, "200x200")
			//if err != nil {
			//	log.WithError(err).Error("")
			//}
			//
			//resizedKey3, err := resizeImages(key, "400x400")
			//if err != nil {
			//	log.WithError(err).Error("")
			//}

			resizedKey := fmt.Sprintf("%s/%s@100x100.jpg", source, fname)
			resizedKey2 := fmt.Sprintf("%s/%s@200x200.jpg", source, fname)
			resizedKey3 := fmt.Sprintf("%s/%s@400x400.jpg", source, fname)

			println("%s : %s, %s, %s", key, resizedKey, resizedKey2, resizedKey3)

			meta, ok := metaMap[fname]
			if !ok {
				metaMap[fname] = &Meta{
					Base: fname,
					Bucket: flagBucket,
					Key: key,
					Key1x: resizedKey,
					Key2x: resizedKey2,
					Key3x: resizedKey3,
					Source: source,
					Type: "image",
				}
			} else {
				meta.Key = key
				meta.Key1x = resizedKey
				meta.Key2x = resizedKey2
				meta.Key3x = resizedKey3
				metaMap[fname] = meta
			}
		case ".gz":
			arr := strings.Split(key, ".")

			if arr[1] == "xml" {
				object := services.NewObject(nil, sctx.Cfg.Bucket, key, 100)
				if err := sctx.Storage.Get(object); err != nil {
					log.WithError(err).Error("")
					return err
				}

				reader, err := gzip.NewReader(object.File)
				if err != nil {
					log.WithError(err)
					return err
				}
				var bytesArr []byte
				if bytesArr, err = ioutil.ReadAll(reader); err != nil {
					log.WithError(err)
					return err
				}

				var meta *Article

				if err = xml.Unmarshal(bytesArr, &meta); err != nil {
					log.Debugf(err.Error())
				}

				sourceId := fmt.Sprintf("%s-1", meta.ID)
				yid := setId(sourceId)

				object.Close()

				value, ok := metaMap[fname]
				if !ok {
					metaMap[fname] = &Meta{
						Base: fname,
						Bucket: flagBucket,
						Yid: yid,
						Source: source,
						Type: "image",
						SourceId: sourceId,
					}
				} else {
					value.Yid = yid
					value.SourceId = sourceId
					metaMap[fname] = value
				}
			}
		}
	}

	err := UploadMetadata(metaMap)
	if err != nil {
		log.WithError(err)
	}

	return nil
}

func main() {

	c := map[string]string{"region": "us-west-2"}
	caws.SetDynamo(c)

	log.SetLevel(log.DebugLevel)

	cfg := &config.Config{
		Bucket:          flagBucket,
		Credentials:     services.NewCredentials(),
		Region:          flagAWSRegion,
		Source:          flagSource,
		ProcessedQueue:  flagQueue,
	}

	sctx, err := services.NewServiceContextWithoutDB(cfg)
	if err != nil {
		log.Debugf(err.Error())
	}

	if err != nil {
		log.Fatal(err.Error())
	}

	keys := utils.RetrieveKeys(sctx, flagTimeFrame)

	err = Process(sctx, keys)
	if err != nil {
		log.WithError(err)
	}
}
