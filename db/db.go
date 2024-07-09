package db

type DB interface {
    FetchTrades(string) ([]TradeSummary, error)
    GetTrade(string, string) (TradeSummary, error) 
}
