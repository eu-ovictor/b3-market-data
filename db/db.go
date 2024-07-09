package db

type DB interface {
	InsertMany([]Trade) error
	FetchTrades(string) ([]TradeSummary, error)
	GetTrade(string, string) (TradeSummary, error)
}
