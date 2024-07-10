package db

type TradeSummary struct {
	Ticker         string  `json:"ticker"`
	MaxRangeValue  float64 `json:"max_range_value"`
	MaxDailyVolume int64   `json:"max_daily_volume"`
}
