package unTrading

import "fmt"

type Parameters struct {
	R     string   //bulk
	Px    string   //bulk
	Ps    string   //bulk
	P     string
	Rg    string
	Cc    string
	Max   string
	Fmt   string
	Type  string   //bulk
	Freq  string   //bulk
	Head  string
	Token string   //bulk
}

func Datarequest(param *Parameters) string {
	return fmt.Sprintf("http://comtrade.un.org/api/get?r=%s&px=%s&ps=%s&p=%s&rg=%s&cc=%s&max=%s&fmt=%s&type=%s&freq=%s&head=%s",
		param.R,
		param.Px,
		param.Ps,
		param.P,
		param.Rg,
		param.Cc,
		param.Max,
		param.Fmt,
		param.Type,
		param.Freq,
		param.Head)
}

func Bulkrequest(param *Parameters) string {
	return fmt.Sprintf("http://comtrade.un.org/api/get/bulk/%s/%s/%s/%s/%s?%s",
		param.Type,
		param.Freq,
		param.Ps,
		param.R,
		param.Px,
		param.Token)
}
