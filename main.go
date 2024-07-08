package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
)

// TODO: read from env
const DATABASE_URL = "postgres://root:passwd@localhost:5432/b3-market-data"
const SELECT = `
    SELECT 
      DISTINCT ON (ticker) t1.ticker, 
      t2.max_range_value, 
      t1.quantity AS max_daily_volume
    FROM 
      trade t1 
      JOIN (
        SELECT 
          ticker, 
          MAX(gross_amount) AS max_range_value 
        FROM 
          trade 
        GROUP BY 
          ticker
      ) t2 ON t1.ticker = t2.ticker 
    ORDER BY 
      t1.ticker, 
      t1.quantity DESC;
`
const SELECT_WHERE_TICKER = ` 
    SELECT 
      DISTINCT ON (ticker) t1.ticker, 
      t2.max_range_value, 
      t1.quantity AS max_daily_volume
    FROM 
      trade t1 
      JOIN (
        SELECT 
          ticker, 
          MAX(gross_amount) AS max_range_value 
        FROM 
          trade 
        WHERE ticker = $1
        GROUP BY 
          ticker
      ) t2 ON t1.ticker = t2.ticker 
    ORDER BY 
      t1.ticker, 
      t1.quantity DESC;
`
const SELECT_WHERE_DATE = ` 
    SELECT DISTINCT ON (ticker)
        t1.ticker,
        t2.max_range_value,
        t1.quantity AS max_daily_volume
    FROM trade t1
    JOIN (
        SELECT ticker, MAX(gross_amount) AS max_range_value
        FROM trade
        WHERE date >= $1
        GROUP BY ticker
    ) t2 ON t1.ticker = t2.ticker
    WHERE t1.date >= $1
    ORDER BY t1.ticker, t1.quantity DESC;
`

const SELECT_WHERE_TICKER_DATE = ` 
    SELECT DISTINCT ON (ticker)
        t1.ticker,
        t2.max_range_value,
        t1.quantity AS max_daily_volume
    FROM trade t1
    JOIN (
        SELECT ticker, MAX(gross_amount) AS max_range_value
        FROM trade
        WHERE date >= $2 AND ticker = $1
        GROUP BY ticker
    ) t2 ON t1.ticker = t2.ticker
    WHERE t1.date >= $2
    ORDER BY t1.ticker, t1.quantity DESC;
`

type TradeSummary struct {
	Ticker         string  `json:"ticker"`
	MaxRangeValue  float64 `json:"max_range_value"`
	MaxDailyVolume int     `json:"max_daily_volume"`
}

func listTradesHandler(c *fiber.Ctx, conn *pgx.Conn) error {
	var (
		rows pgx.Rows
		err  error
	)

	date := c.Query("date")
	if date != "" {
		rows, err = conn.Query(c.Context(), SELECT_WHERE_DATE, date)
	} else {
		rows, err = conn.Query(c.Context(), SELECT)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return c.SendStatus(http.StatusInternalServerError)
	}
	defer rows.Close()

	var trades []TradeSummary

	for rows.Next() {
		var trade TradeSummary

		err := rows.Scan(&trade.Ticker, &trade.MaxRangeValue, &trade.MaxDailyVolume)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return c.SendStatus(http.StatusInternalServerError)
		}

		trades = append(trades, trade)
	}

	if rows.Err() != nil {
		fmt.Fprintln(os.Stderr, err)
		return c.SendStatus(http.StatusInternalServerError)
	}

	// TODO: add content negotiation
	responseBody, err := json.Marshal(trades)
	if err != nil {
		return c.SendStatus(http.StatusInternalServerError)
	}

	c.Set("Content-Type", "application/json")

	return c.Send(responseBody)
}

func detailTradesHandler(c *fiber.Ctx, conn *pgx.Conn) error {
	ticker := c.Params("ticker")

	var row pgx.Row

	date := c.Query("date")

	if date != "" {
		row = conn.QueryRow(c.Context(), SELECT_WHERE_TICKER_DATE, ticker, date)
	} else {
		row = conn.QueryRow(c.Context(), SELECT_WHERE_TICKER, ticker)
	}

	var trade TradeSummary

	err := row.Scan(&trade.Ticker, &trade.MaxRangeValue, &trade.MaxDailyVolume)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return c.SendStatus(http.StatusInternalServerError)
	}

	// TODO: add content negotiation
	responseBody, err := json.Marshal(trade)
	if err != nil {
		return c.SendStatus(http.StatusInternalServerError)
	}

	c.Set("Content-Type", "application/json")

	return c.Send(responseBody)
}

func main() {
	conn, err := pgx.Connect(context.Background(), DATABASE_URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	app := fiber.New()

	app.Get("/trades", func(c *fiber.Ctx) error {
		return listTradesHandler(c, conn)
	})

	app.Get("/trades/:ticker", func(c *fiber.Ctx) error {
		return detailTradesHandler(c, conn)
	})

	if err := app.Listen(":8000"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
