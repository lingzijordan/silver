package formats

type UNJson struct {
	Validation struct {
			   Status struct {
					  Name string `json:"name"`
					  Value int `json:"value"`
					  Category int `json:"category"`
					  Description string `json:"description"`
					  HelpURL string `json:"helpUrl"`
				  } `json:"status"`
			   Message interface{} `json:"message"`
			   Count struct {
					  Value int `json:"value"`
					  Started string `json:"started"`
					  Finished string `json:"finished"`
					  DurationSeconds float64 `json:"durationSeconds"`
				  } `json:"count"`
			   DatasetTimer struct {
					  Started string `json:"started"`
					  Finished string `json:"finished"`
					  DurationSeconds float64 `json:"durationSeconds"`
				  } `json:"datasetTimer"`
		   } `json:"validation"`
	Dataset []struct {
		PfCode string `json:"pfCode"`
		Yr int `json:"yr"`
		Period int `json:"period"`
		PeriodDesc string `json:"periodDesc"`
		AggrLevel int `json:"aggrLevel"`
		IsLeaf int `json:"IsLeaf"`
		RgCode int `json:"rgCode"`
		RgDesc string `json:"rgDesc"`
		RtCode int `json:"rtCode"`
		RtTitle string `json:"rtTitle"`
		Rt3ISO string `json:"rt3ISO"`
		PtCode int `json:"ptCode"`
		PtTitle string `json:"ptTitle"`
		Pt3ISO string `json:"pt3ISO"`
		PtCode2 interface{} `json:"ptCode2"`
		PtTitle2 string `json:"ptTitle2"`
		Pt3ISO2 string `json:"pt3ISO2"`
		CstCode string `json:"cstCode"`
		CstDesc string `json:"cstDesc"`
		MotCode string `json:"motCode"`
		MotDesc string `json:"motDesc"`
		CmdCode string `json:"cmdCode"`
		CmdDescE string `json:"cmdDescE"`
		QtCode int `json:"qtCode"`
		QtDesc string `json:"qtDesc"`
		QtAltCode interface{} `json:"qtAltCode"`
		QtAltDesc string `json:"qtAltDesc"`
		TradeQuantity interface{} `json:"TradeQuantity"`
		AltQuantity interface{} `json:"AltQuantity"`
		NetWeight interface{} `json:"NetWeight"`
		GrossWeight interface{} `json:"GrossWeight"`
		TradeValue uint64 `json:"TradeValue"`
		CIFValue interface{} `json:"CIFValue"`
		FOBValue interface{} `json:"FOBValue"`
		EstCode int `json:"estCode"`
	} `json:"dataset"`
}

type UnBulk struct {
	Classification         string
	Year                   string
	Period                 string
	PeriodDesc             string
	AggregateLevel         string
	IsLeafCode             string
	TradeFlowCode          string
	TradeFlow              string
	ReporterCode           string
	Reporter               string
	ReporterISO            string
	PartnerCode            string
	Partner                string
	PartnerISO             string
	CommodityCode          string
	Commodity              string
	TradeValueUS           string
	Flag                   string
}

type UnBulkHS struct {
	Classification         string
	Year                   int
	Period                 int
	PeriodDesc             int
	AggregateLevel         int
	IsLeafCode             int
	TradeFlowCode          int
	TradeFlow              string
	ReporterCode           int
	Reporter               string
	ReporterISO            string
	PartnerCode            int
	Partner                string
	PartnerISO             string
	CommodityCode          int
	Commodity              string
	QtyUnitCode            int
	QtyUnit                string
	Qty                    int
	NetweightKg            int
	TradeValueUS           int
	Flag                   int
}
