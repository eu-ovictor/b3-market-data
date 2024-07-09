package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
)

const defaultPort = "8000"

const SELECT = `
    SELECT 
      ticker, 
      MAX(max_range_value) AS max_range_value, 
      MAX(total_quantity) AS max_daily_volume 
    FROM 
      trade_summary 
    GROUP BY 
      ticker;
`
const SELECT_WHERE_TICKER = ` 
    SELECT 
      ticker, 
      MAX(max_range_value) AS max_range_value, 
      MAX(total_quantity) AS max_daily_volume 
    FROM 
      trade_summary 
    WHERE ticker = $1
    GROUP BY 
      ticker;
`
const SELECT_WHERE_DATE = ` 
    SELECT 
      ticker, 
      MAX(max_range_value) AS max_range_value, 
      MAX(total_quantity) AS max_daily_volume 
    FROM 
      trade_summary 
    WHERE date >= $1
    GROUP BY 
      ticker;
`

const SELECT_WHERE_TICKER_DATE = ` 
    SELECT 
      ticker, 
      MAX(max_range_value) AS max_range_value, 
      MAX(total_quantity) AS max_daily_volume 
    FROM 
      trade_summary 
    WHERE ticker = $1 AND date >= $2
    GROUP BY 
      ticker;
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
        if err == pgx.ErrNoRows {
            noTrade := TradeSummary{
                Ticker: ticker,
                MaxRangeValue: 0,
                MaxDailyVolume: 0,
            }

            // TODO: add content negotiation
            responseBody, err := json.Marshal(noTrade)
            if err != nil {
                return c.SendStatus(http.StatusInternalServerError)
            }

	        c.Set("Content-Type", "application/json")

	        return c.Send(responseBody)
        }
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

func serve(u string, p string) error {
	conn, err := pgx.Connect(context.Background(), u)
	if err != nil {
        return err
	}
	defer conn.Close(context.Background())

	app := fiber.New()

	app.Get("/trades", func(c *fiber.Ctx) error {
		return listTradesHandler(c, conn)
	})

	app.Get("/trades/:ticker", func(c *fiber.Ctx) error {
		return detailTradesHandler(c, conn)
	})

    if err := app.Listen(fmt.Sprintf(":%s", p)); err != nil {
        return err
	}

    return nil
}

var port string 

var apiCmd = &cobra.Command {
    Use: "api",
    Short: "Spins up the web API",
    RunE: func(_ *cobra.Command, _ []string) error {
        u, err := loadDatabaseURI()
        if err != nil {
            return err 
        }

        if port == "" {
			port = os.Getenv("PORT")
		}

		if port == "" {
			port = defaultPort
		}

        return serve(u, port)
    },
}

func apiCLI() *cobra.Command {
    apiCmd.Flags().StringVarP(&port, "port", "p", defaultPort, fmt.Sprintf("web server port (default PORT environment variable or %s)", defaultPort))

    return apiCmd
}
