package main

import (
	"os"
	"io/ioutil"
	"github.com/yewno/log"
	"encoding/xml"
	"io"
	"fmt"
	"strings"
	"github.com/davecgh/go-spew/spew"
)

type GaleManuscriptMeta struct {
	XMLName  xml.Name `xml:"manuscript"`
	MsInfo struct {
				 PSMID string `xml:"PSMID"`
				 Text  string `xml:",innerxml"`
			 } `xml:"msInfo"`
	Notes struct {
				 Text string `xml:",innerxml"`
			 } `xml:"msNotes"`
}

type Page struct {
	XMLName  xml.Name `xml:"page"`
	InnerXML string   `xml:",innerxml"`
}

type ManuscriptMeta struct {
	XMLName  xml.Name `xml:"manuscript"`
	MsInfo struct {
				 PSMID string `xml:"PSMID"`
				 Text  string `xml:",innerxml"`
			 } `xml:"msInfo"`
	Notes struct {
				 Text string `xml:",innerxml"`
			 } `xml:"msNotes"`
	Text     Page `xml:"page"`
}

func main() {

	f, err := os.Open("/Users/ziling/Documents/gale/SAS1/Manuscripts/SASO0108-C00001-M0000006.xml")
	if err != nil {
		log.WithError(err)
	}

	res, err := ioutil.ReadAll(f)

	//spew.Dump(string(res))

	if err != nil {
		log.Debugf(err.Error())
	}
	var grandMeta GaleManuscriptMeta

	if err = xml.Unmarshal(res, &grandMeta); err != nil {
		log.Debugf(err.Error())
	}
	//decoder.CharsetReader = charset.NewReaderLabel
	//err = decoder.Decode(&grandMeta)

	decoder := xml.NewDecoder(strings.NewReader(string(res)))
//	decoder.CharsetReader = charset.NewReaderLabel

	spew.Dump(grandMeta)

	var counter int

	for {

		t, err := decoder.Token()

		if err == io.EOF {
			break
		} else if err != nil {
			log.Debugf(err.Error())
		}
		switch se := t.(type) {
		case xml.StartElement:
			if se.Name.Local == "page" {
				counter++
				var page Page
				decoder.DecodeElement(&page, &se)

				spew.Dump(page)

				meta := &ManuscriptMeta{
					XMLName: grandMeta.XMLName,
					MsInfo: grandMeta.MsInfo,
					Notes: grandMeta.Notes,
					Text: page,
				}

				response, err := xml.Marshal(meta)

				flagFile := fmt.Sprintf("/Users/ziling/Desktop/test/%d.xml", counter)
				file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
				if err != nil {
					log.Debugf(err.Error())
				}
				defer file.Close()

				_, err = file.Write(response)
				if err != nil {
					log.Debugf(err.Error())
				}

				if _, err := file.Seek(0, 0); err != nil {
					log.Debugf(err.Error())
				}
			}
		}
	}

	//bytesArr = bytes.Replace(bytesArr, []byte("</named-content>"), []byte(" "), -1)
	//bytesArr = bytes.Replace(bytesArr, []byte("<named-content"), []byte(" "), -1)

	// common cleanup

	//bytesArr = carbon.CleanXML(bytesArr)
	////bytesArr = bytes.Replace(bytesArr, []byte("&"), []byte(""), -1)
	////fmt.Println(string(bytesArr))
	////str, _ := carbon.CleanHTML(string(bytesArr))
	//
	////fmt.Println(str)
	//
	////log.Debugf(string(bytesArr))
	//meta := new(CengageMeta)
	//
	//if err = xml.Unmarshal(bytesArr, &meta); err != nil {
	//	log.Debugf(err.Error())
	//}
	//
	//spew.Dump(meta)
	//
	////page := meta.Text
	////Body, _ := carbon.CleanHTML(page.Text)
	////fmt.Println(Body)
	//
	//metaArr := []*PageMeta{}
	//
	//for _, page := range meta.Text {
	//	singlePage := &PageMeta{
	//		XMLName: meta.XMLName,
	//		Citation: meta.Citation,
	//		BookInfo: meta.BookInfo,
	//		Text: meta.Text,
	//	}
	//
	//	metaArr = append(metaArr, singlePage)
}

	//spew.Dump(meta)

	//for i, page := range metaArr {
	//	flagFile := fmt.Sprintf("/Users/ziling/Desktop/%s.xml", i)
	//	file, err := os.OpenFile(flagFile, os.O_CREATE | os.O_WRONLY, 0644)
	//	if err != nil {
	//		log.WithError(err)
	//	}
	//	defer file.Close()
	//
	//	response, err := xml.Marshal(page)
	//	if err != nil {
	//		log.WithError(err)
	//	}
	//
	//	_, err = file.Write(response)
	//	if err != nil {
	//		log.WithError(err)
	//	}
	//
	//	if _, err := file.Seek(0, 0); err != nil {
	//		log.WithError(err).Error("")
	//	}
	//}

