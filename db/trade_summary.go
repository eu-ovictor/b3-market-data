package db

type TradeSummary struct {
	Ticker         string  `json:"ticker"`
	MaxRangeValue  float64 `json:"max_range_value"`
	MaxDailyVolume int     `json:"max_daily_volume"`
}
