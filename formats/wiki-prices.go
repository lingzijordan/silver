package formats

//ticker,date,open,high,low,close,volume,ex-dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume
type WikiPrices struct {
	Ticker             string
	Date               string
	Open               string
	High               string
	Low                string
	Close              string
	Volume             string
	ExDividend         string
	SplitRatio         string
	AdjOpen            string
	AdjHigh            string
	AdjLow             string
	AdjClose           string
	AdjVolume          string
}
