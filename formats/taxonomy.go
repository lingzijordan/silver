package formats

type Industries struct {
	Industry          string `json:"industry"`
	Source            string `json:"source"`
	Leaders           []struct {
		Name   string `json:"name"`
		Symbol string `json:"symbol"`
		Value  string `json:"value"`
	} `json:"leaders"`
	Laggards          []struct {
		Name   string `json:"name"`
		Symbol string `json:"symbol"`
		Value  string `json:"value"`
	} `json:"laggards"`
	AllCompanies      []struct {
		Name   string `json:"name"`
		Symbol string `json:"symbol"`
	} `json:"all-companies"`
	RelatedIndustries []string `json:"related-industries"`
}

type ISIN struct {
	Data []struct {
		Isin     string `json:"isin"`
		Mappings []struct {
			Symbol   string `json:"symbol"`
			Name     string `json:"name"`
			Type     string `json:"type"`
			Exchange string `json:"exchange"`
			Country  string `json:"country"`
		} `json:"mappings"`
	} `json:"data"`
}

type SP500 struct {
	Ticker      string
	CompanyName string
	Isin        string
	Cusip       string
}

type IsinUpdate struct {
	Isin   string
	Ticker string
}

type IsinRecord struct {
	Isin            string
	CompanyName     string
	CompanySymbol   string
	CompanyType     string
	CompanyExchange string
}
