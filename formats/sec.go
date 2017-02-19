package formats

type SecMeta struct {
	YId    string `json:"yId"`
	Date   string `json:"date"`
	Source string `json:"source"`
	Ticker string `json:"ticker"`
	Cik    string `json:"cik"`
}

type CikXml struct {
	Cik string `xml:"company-info>cik"`
}

type CikMapping struct {
	Cik string
	Ticker string
}

type SecContentMeta struct {
	YId                  string        `json:"yId"` // primary key
	Created              string        `json:"created,omitempty"`
	Date                 string        `json:"date"`
	Day                  int           `json:"day"`
	Month                int           `json:"month"`
	Year                 int           `json:"year"`
	Language             string        `json:"language"`
	FullText             string        `json:"fulltext,omitempty"`
	Type                 string        `json:"type"`
	IngestedAt           string        `json:"ingestedAt"`
	IndexedAt            string        `json:"IndexedAt,omitempty"`
	ProcessedAt          string        `json:"processedAt,omitempty"`
	Source               string        `json:"source"`
	Title                string        `json:"title,omitempty"`
	Cik                  string        `json:"cik"`
	Ticker               string        `json:"ticker,omitempty"`
	Isin                 string        `json:"isin,omitempty"`
	Hashcode             string        `json:"hashcode"`
}