package formats

type ReportingArea struct {
	More bool `json:"more"`
	Results []struct {
		ID string `json:"id"`
		Text string `json:"text"`
	} `json:"results"`
}
