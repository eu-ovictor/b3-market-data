package loader

import (
	"strconv"
	"strings"
	"time"

	"github.com/eu-ovictor/b3-market-data/db"
)

func parseEntryTime(s string) (time.Time, error) {
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		return time.Time{}, err
	}

	hour, _ := strconv.Atoi(s[0:2])
	minute, _ := strconv.Atoi(s[2:4])
	second, _ := strconv.Atoi(s[4:6])
	nano, _ := strconv.Atoi(s[6:9])

	entryTime := time.Date(0, 1, 1, hour, minute, second, nano*1000000, loc)

	return entryTime, nil
}

func parseGrossAmount(s string) (float64, error) {
	s = strings.Replace(s, ",", ".", 1)

	return strconv.ParseFloat(s, 64)
}

func processRow(row []string) (db.Trade, error) {
	ticker := row[1]

	grossAmount, err := parseGrossAmount(row[3])
	if err != nil {
		return db.Trade{}, err
	}

	quantity, err := strconv.ParseInt(row[4], 10, 64)
	if err != nil {
		return db.Trade{}, err
	}

	entryTime, err := parseEntryTime(row[5])
	if err != nil {
		return db.Trade{}, err
	}

	layout := "2006-01-02"
	date, err := time.Parse(layout, row[8])
	if err != nil {
		return db.Trade{}, err
	}

	return db.Trade{
		Ticker:      ticker,
		GrossAmount: grossAmount,
		Quantity:    quantity,
		EntryTime:   entryTime,
		Date:        date,
	}, nil
}
