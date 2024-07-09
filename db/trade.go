package db

import "time"

type Trade struct {
	Ticker      string
	GrossAmount float64
	Quantity    int64
	EntryTime   time.Time
	Date        time.Time
}
