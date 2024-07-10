package db

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const TICKER = "TICK3R"
const ANOTHER_TICKER = "4N0TH3RTICK3R"

var TEST_DATABASE_URL string = os.Getenv("TEST_DATABASE_URL")

func TestMaxRangeValue(t *testing.T) {
	var (
		lower_quantity   int64   = 1
		lower_amount     float64 = 1
		average_amount   float64 = 1.5
		average_quantity int64   = 2
		higher_amount    float64 = 2
		higher_quantity  int64   = 3
	)

	trades := []Trade{
		{
			Ticker:      TICKER,
			GrossAmount: lower_amount,
			Quantity:    higher_quantity,
			EntryTime:   time.Now().AddDate(0, 0, -2),
			Date:        time.Now().AddDate(0, 0, -2),
		},
		{
			Ticker:      TICKER,
			GrossAmount: average_amount,
			Quantity:    average_quantity,
			EntryTime:   time.Now().AddDate(0, 0, -1),
			Date:        time.Now().AddDate(0, 0, -1),
		},
		{
			Ticker:      TICKER,
			GrossAmount: higher_amount,
			Quantity:    lower_quantity,
			EntryTime:   time.Now(),
			Date:        time.Now(),
		},
	}

	pg, err := NewPostgreSQL(TEST_DATABASE_URL)
	assert.NoError(t, err, "expected no error connecting to postgres, got %s", err)
	defer func() {
		if err := pg.DropTable(); err != nil {
			t.Errorf("expected no error dropping the table, got %s", err)
		}
		pg.Close()
	}()

	err = pg.CreateTable()
	assert.NoError(t, err, "expected no error creating table in postgres, got %s", err)

	err = pg.InsertMany(trades)
	assert.NoError(t, err, "expected no error inserting trades, got %s", err)

	err = pg.PostLoad()
	assert.NoError(t, err, "expected no error creating views in postgres, got %s", err)

	summaries, err := pg.FetchTrades("")
	assert.NoError(t, err, "expected no error fetching trades, got %s", err)
	assert.Equal(t, len(summaries), 1, "expected a single summary by ticker, got %v", summaries)

	summary := summaries[0]
	assert.Equal(t, summary.MaxRangeValue, higher_amount, "expected max range value to be %v, got %v", higher_amount, summary.MaxRangeValue)
	assert.Equal(t, summary.MaxRangeValue, higher_amount, "expected max daily volume to be %v, got %v", higher_amount, summary.MaxRangeValue)
}

func TestMaxDailyVolume(t *testing.T) {
	var (
		amount                 float64 = 1
		expectedMaxDailyVolume int     = 40
	)

	trades := []Trade{
		{
			Ticker:      TICKER,
			GrossAmount: float64(amount),
			Quantity:    20,
			EntryTime:   time.Now().AddDate(0, 0, -1),
			Date:        time.Now().AddDate(0, 0, -1),
		},
		{
			Ticker:      TICKER,
			GrossAmount: amount,
			Quantity:    10,
			EntryTime:   time.Now(),
			Date:        time.Now(),
		},
		{
			Ticker:      TICKER,
			GrossAmount: amount,
			Quantity:    30,
			EntryTime:   time.Now(),
			Date:        time.Now(),
		},
	}

	pg, err := NewPostgreSQL(TEST_DATABASE_URL)
	assert.NoError(t, err, "expected no error connecting to postgres, got %s", err)
	defer func() {
		if err := pg.DropTable(); err != nil {
			t.Errorf("expected no error dropping the table, got %s", err)
		}
		pg.Close()
	}()

	err = pg.CreateTable()
	assert.NoError(t, err, "expected no error creating table in postgres, got %s", err)

	err = pg.InsertMany(trades)
	assert.NoError(t, err, "expected no error inserting trades, got %s", err)

	err = pg.PostLoad()
	assert.NoError(t, err, "expected no error creating views in postgres, got %s", err)

	summaries, err := pg.FetchTrades("")
	assert.NoError(t, err, "expected no error fetching trades, got %s", err)
	assert.Equal(t, len(summaries), 1, "expected a single trade by ticker, got %v", summaries)

	summary := summaries[0]
	assert.Equal(t, summary.MaxDailyVolume, expectedMaxDailyVolume, "expected max daily volume to be %v, got %v", expectedMaxDailyVolume, summary.MaxDailyVolume)
}

func TestGetByTicker(t *testing.T) {
	expectedTrade := Trade{
		Ticker:      ANOTHER_TICKER,
		GrossAmount: 1,
		Quantity:    10,
		EntryTime:   time.Now(),
		Date:        time.Now(),
	}

	trades := []Trade{
		{
			Ticker:      TICKER,
			GrossAmount: 1,
			Quantity:    20,
			EntryTime:   time.Now().AddDate(0, 0, -1),
			Date:        time.Now().AddDate(0, 0, -1),
		},
		expectedTrade,
	}

	pg, err := NewPostgreSQL(TEST_DATABASE_URL)
	assert.NoError(t, err, "expected no error connecting to postgres, got %s", err)
	defer func() {
		if err := pg.DropTable(); err != nil {
			t.Errorf("expected no error dropping the table, got %s", err)
		}
		pg.Close()
	}()

	err = pg.CreateTable()
	assert.NoError(t, err, "expected no error creating table in postgres, got %s", err)

	err = pg.InsertMany(trades)
	assert.NoError(t, err, "expected no error inserting trades, got %s", err)

	err = pg.PostLoad()
	assert.NoError(t, err, "expected no error creating views in postgres, got %s", err)

	summary, err := pg.GetTrade(ANOTHER_TICKER, "")
	assert.NoError(t, err, "expected no error getting summary, got %s", err)

	assert.Equal(t, summary.Ticker, expectedTrade.Ticker, "expected summary ticker to be %v, got %v", expectedTrade.Ticker, summary.Ticker)
	assert.Equal(t, summary.MaxRangeValue, expectedTrade.GrossAmount, "expected max range value to be %v, got %v", expectedTrade.GrossAmount, summary.MaxRangeValue)
	assert.Equal(t, summary.MaxDailyVolume, expectedTrade.Quantity, "expected max daily volume to be %v, got %v", expectedTrade.Quantity, summary.MaxDailyVolume)
}

func TestGetByTickerAndDate(t *testing.T) {
	expectedTrade := Trade{
		Ticker:      TICKER,
		GrossAmount: 0.5,
		Quantity:    1,
		EntryTime:   time.Now(),
		Date:        time.Now(),
	}

	trades := []Trade{
		{
			Ticker:      ANOTHER_TICKER,
			GrossAmount: 1,
			Quantity:    10,
			EntryTime:   time.Now(),
			Date:        time.Now(),
		},
		{
			Ticker:      TICKER,
			GrossAmount: 1,
			Quantity:    20,
			EntryTime:   time.Now().AddDate(0, 0, -1),
			Date:        time.Now().AddDate(0, 0, -1),
		},
		expectedTrade,
	}

	pg, err := NewPostgreSQL(TEST_DATABASE_URL)
	assert.NoError(t, err, "expected no error connecting to postgres, got %s", err)
	defer func() {
		if err := pg.DropTable(); err != nil {
			t.Errorf("expected no error dropping the table, got %s", err)
		}
		pg.Close()
	}()

	err = pg.CreateTable()
	assert.NoError(t, err, "expected no error creating table in postgres, got %s", err)

	err = pg.InsertMany(trades)
	assert.NoError(t, err, "expected no error inserting trades, got %s", err)

	err = pg.PostLoad()
	assert.NoError(t, err, "expected no error creating views in postgres, got %s", err)

	summary, err := pg.GetTrade(TICKER, time.Now().Format("2006-01-02"))
	assert.NoError(t, err, "expected no error getting summary, got %s", err)

	assert.Equal(t, summary.Ticker, expectedTrade.Ticker, "expected summary ticker to be %v, got %v", expectedTrade.Ticker, summary.Ticker)
	assert.Equal(t, summary.MaxRangeValue, expectedTrade.GrossAmount, "expected max range value to be %v, got %v", expectedTrade.GrossAmount, summary.MaxRangeValue)
	assert.Equal(t, summary.MaxDailyVolume, expectedTrade.Quantity, "expected max daily volume to be %v, got %v", expectedTrade.Quantity, summary.MaxDailyVolume)
}

func TestFetchByDate(t *testing.T) {
	expectedTrade1 := Trade{
		Ticker:      ANOTHER_TICKER,
		GrossAmount: 0.2,
		Quantity:    1,
		EntryTime:   time.Now(),
		Date:        time.Now(),
	}

	expectedTrade2 := Trade{
		Ticker:      TICKER,
		GrossAmount: 0.3,
		Quantity:    1,
		EntryTime:   time.Now(),
		Date:        time.Now(),
	}

	expectedTrades := map[string]Trade{
		ANOTHER_TICKER: expectedTrade1,
		TICKER:         expectedTrade2,
	}

	trades := []Trade{
		{
			Ticker:      ANOTHER_TICKER,
			GrossAmount: 1,
			Quantity:    10,
			EntryTime:   time.Now().AddDate(0, 0, -1),
			Date:        time.Now().AddDate(0, 0, -1),
		},
		expectedTrade1,
		{
			Ticker:      TICKER,
			GrossAmount: 1,
			Quantity:    20,
			EntryTime:   time.Now().AddDate(0, 0, -1),
			Date:        time.Now().AddDate(0, 0, -1),
		},
		expectedTrade2,
	}

	pg, err := NewPostgreSQL(TEST_DATABASE_URL)
	assert.NoError(t, err, "expected no error connecting to postgres, got %s", err)
	defer func() {
		if err := pg.DropTable(); err != nil {
			t.Errorf("expected no error dropping the table, got %s", err)
		}
		pg.Close()
	}()

	err = pg.CreateTable()
	assert.NoError(t, err, "expected no error creating table in postgres, got %s", err)

	err = pg.InsertMany(trades)
	assert.NoError(t, err, "expected no error inserting trades, got %s", err)

	err = pg.PostLoad()
	assert.NoError(t, err, "expected no error creating views in postgres, got %s", err)

	summaries, err := pg.FetchTrades(time.Now().Format("2006-01-02"))
	assert.NoError(t, err, "expected no error fetching summaries, got %s", err)
	assert.Equal(t, len(summaries), 2, "expected a single trade by ticker, got %v", summaries)

	for _, summary := range summaries {
		expectedTrade := expectedTrades[summary.Ticker]

		assert.Equal(t, summary.Ticker, expectedTrade.Ticker, "expected summary ticker to be %v, got %v", expectedTrade.Ticker, summary.Ticker)
		assert.Equal(t, summary.MaxRangeValue, expectedTrade.GrossAmount, "expected max range value to be %v, got %v", expectedTrade.GrossAmount, summary.MaxRangeValue)
		assert.Equal(t, summary.MaxDailyVolume, expectedTrade.Quantity, "expected max daily volume to be %v, got %v", expectedTrade.Quantity, summary.MaxDailyVolume)
	}

}
