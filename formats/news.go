package formats

type NewsContentMeta struct {
	YId                  string        `json:"yId"` // primary key
	Created              string        `json:"created"`
	Date                 string        `json:"date"`
	Day                  int           `json:"day"`
	Month                int           `json:"month"`
	Year                 int           `json:"year"`
	Language             string        `json:"language"`
	Headline             string        `json:"headline,omitempty"`
	FullText             string        `json:"fulltext,omitempty"`
	Type                 string        `json:"type"`
	IngestedAt           string        `json:"ingestedAt"`
	IndexedAt            string        `json:"IndexedAt,omitempty"`
	ProcessedAt          string        `json:"processedAt,omitempty"`
	Source               string        `json:"source"`
	Title                string        `json:"title,omitempty"`
	Keywords             string        `json:"keywords,omitempty"`
	Topics               string        `json:"topics"`
	NamedItems           string        `json:"namedItems,omitempty"`
	NamedItemsOriginal   string        `json:"namedItemsOriginal,omitempty"`
}

type NewsContent struct {
	Date                 string
	Time                 string
	UNIQUE_STORY_INDEX   string
	EVENT_TYPE           string
	PNAC                 string
	STORY_DATE_TIME      string
	TAKE_DATE_TIME       string
	HEADLINE_ALERT_TEXT  string
	ACCUMULATED_STORY_TEXT string
	TAKE_TEXT            string
	PRODUCTS             string
	TOPICS               string
	RELATED_RICS         string    //tickers
	NAMED_ITEMS          string
	HEADLINE_SUBTYPE     string
	STORY_TYPE           string
	TABULAR_FLAG         string
	ATTRIBUTION          string
	LANGUAGE             string
}

type NewsUpdateMeta struct {
	YId                  string        `json:"yId"` // primary key
	Created              string        `json:"created"`
	Date                 string        `json:"date"`
	Day                  int           `json:"day"`
	Month                int           `json:"month"`
	Year                 int           `json:"year"`
	Language             string        `json:"language"`
	Headline             string        `json:"headline,omitempty"`
	Type                 string        `json:"type"`
	IngestedAt           string        `json:"ingestedAt"`
	IndexedAt            string        `json:"IndexedAt,omitempty"`
	ProcessedAt          string        `json:"processedAt,omitempty"`
	Source               string        `json:"source"`
	Tickers              string        `json:"tickers,omitempty"`
}
