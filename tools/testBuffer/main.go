package main

import (
	"fmt"
	"net/url"
)

func main() {
	hans := "http://handemo.hh-software.com/hanapi/"
	documentUrl := "http://www.sosyalarastirmalar.com/cilt6/cilt6sayi28_pdf/Njemanze.pdf"
	s := fmt.Sprintf("%s?method=getHANID&id=search&url=%s", hans, url.QueryEscape(documentUrl))

	fmt.Println(s)

}
