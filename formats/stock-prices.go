package formats

type StockPricesTable struct {
	Ticker             string
	Date               string
	Open               string
	High               string
	Low                string
	Close              string
	Volume             string
}

type StockPrices struct {
	Date               string
	Open               string
	High               string
	Low                string
	Close              string
	Volume             string
	place1             string
	place2             string
	place3             string
}