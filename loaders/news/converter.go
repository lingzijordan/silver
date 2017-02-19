package news

import (
	"strings"
	"fmt"
	"strconv"
	"crypto/sha1"
	"io"
	"time"
	"github.com/yewno/silver/formats"
	"github.com/yewno/silver/services"
)

func ConvertNewsEntry(entry *formats.NewsContent, sctx *services.ServiceContext) *formats.NewsContentMeta {
	parseDate := strings.Split(entry.Date, "-")
	topics := fmt.Sprintf("%s %s", entry.PRODUCTS, entry.TOPICS)
	var tickers []string
	var originalTickers []string
	if entry.RELATED_RICS != "" {
		originalTickers = strings.Split(entry.RELATED_RICS, " ")

		for _, ticker := range originalTickers {
			tickers = append(tickers, strings.Split(ticker, ".")[0])
		}
	}

	day, _ := strconv.Atoi(parseDate[2])
	month, _ := strconv.Atoi(parseDate[1])
	year, _ := strconv.Atoi(parseDate[0])

	hash := sha1.New()
	hashString := fmt.Sprintf("%s%s", sctx.Cfg.Source, entry.UNIQUE_STORY_INDEX)
	io.WriteString(hash, hashString)
	yid := fmt.Sprintf("%x", hash.Sum(nil))[0:32]

	return &formats.NewsContentMeta{
		YId:                  yid,
		Created:              time.Now().String(),
		Date:                 entry.STORY_DATE_TIME,
		Day:                  day,
		Month:                month,
		Year:                 year,
		Language:             entry.LANGUAGE,
		Headline:             entry.HEADLINE_ALERT_TEXT,
		FullText:             entry.TAKE_TEXT,
		Type:                 entry.EVENT_TYPE,
		IngestedAt:           time.Now().String(),
		Source:               sctx.Cfg.Source,
		Title:                entry.HEADLINE_ALERT_TEXT,
		Topics:               topics,
		NamedItems:           strings.Join(tickers, " "),
		NamedItemsOriginal:   entry.RELATED_RICS,
	}
}
