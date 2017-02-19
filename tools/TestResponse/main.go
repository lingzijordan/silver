package main

import (
	"os"
	"github.com/yewno/log"
	"bufio"
	"strings"
	"fmt"
	"github.com/davecgh/go-spew/spew"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type TickerIsin struct {
	Ticker string
	Isin   string
}

func main() {
	key := "/Users/ziling/isinTicker"
	f, err := os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	mapping := make(map[string]int)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")

		//isin := arr[0]
		ticker := arr[1]

		count, ok := mapping[ticker]
		if !ok {
			mapping[ticker] = 1
		} else {
			mapping[ticker] = count + 1
		}
	}

	var s string
	for k, v := range mapping {
		if v > 1 {
			s = fmt.Sprintf("%s;%s;", s, k)
		}
	}

	fmt.Println(s)

	tickerCik := make(map[string]string)
	key = "/Users/ziling/cikTicker.txt"
	f2, err := os.Open(key)
	if err != nil {
		log.Debugf(err.Error())
	}
	defer f.Close()
	scanner2 := bufio.NewScanner(f2)

	for scanner2.Scan() {
		line := scanner2.Text()
		arr := strings.Split(line, "\t")

		cik := arr[0]
		ticker := arr[1]

		fmt.Println("%s/%s", cik, ticker)

		c, ok := tickerCik[ticker]
		if !ok {
			tickerCik[ticker] = cik
		} else {
			tickerCik[ticker] = c
		}
	}

	spew.Dump(tickerCik)

	tickerIsin := make(map[string]string)
	key = "/Users/ziling/isinTicker"
	f, err = os.Open(key)
	if err != nil {
		log.WithError(err)
	}
	defer f.Close()
	scanner = bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		arr := strings.Split(line, "\t")

		isin := arr[0]
		ticker := arr[1]

		testS := fmt.Sprintf(";%s;", ticker)
		if strings.Contains(s, testS) {continue}

		c, ok := tickerIsin[ticker]
		if !ok {
			tickerIsin[ticker] = isin
		} else {
			tickerIsin[ticker] = c
		}
	}

	resultMap := make(map[string]*TickerIsin)

	for ticker, cik := range tickerCik {
		isin, ok := tickerIsin[ticker]
		if !ok {
			fmt.Println("ticker %s without isin", ticker)
			continue
		} else {
			t := &TickerIsin{
				Ticker: ticker,
				Isin: isin,
			}
			resultMap[cik] = t
		}
	}

	flagFile := "/Users/ziling/resultMap3.txt"
	file, err := os.OpenFile(flagFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithError(err)
	}
	defer file.Close()

	for k, v := range resultMap {
		s := fmt.Sprintf("%s,%s,%s\n", k, v.Ticker, v.Isin)
		_, err := file.WriteString(s)
		if err != nil {
			log.WithError(err)
			continue
		}
	}

	if _, err := file.Seek(0, 0); err != nil {
		log.WithError(err).Error("")
	}

}
