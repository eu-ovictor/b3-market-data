package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/eu-ovictor/b3-market-data/db"
	"github.com/gofiber/fiber/v2"
)

type api struct {
	db db.DB
}

func (app *api) fetchTradesHandler(c *fiber.Ctx) error {
	date := c.Query("date")

	trades, err := app.db.FetchTrades(date)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return c.SendStatus(http.StatusInternalServerError)
	}

	responseBody, err := json.Marshal(trades)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return c.SendStatus(http.StatusInternalServerError)
	}

	c.Set("Content-Type", "application/json")

	return c.Send(responseBody)
}

func (app *api) getTradeHandler(c *fiber.Ctx) error {
	ticker := c.Params("ticker")

	date := c.Query("date")

	trade, err := app.db.GetTrade(ticker, date)
	if err != nil {
		return c.SendStatus(http.StatusInternalServerError)

	}

	responseBody, err := json.Marshal(trade)
	if err != nil {
		return c.SendStatus(http.StatusInternalServerError)
	}

	c.Set("Content-Type", "application/json")

	return c.Send(responseBody)
}

func Serve(db db.DB, p string) error {
	if !strings.HasPrefix(p, ":") {
		p = ":" + p
	}

	app := api{
		db: db,
	}

	router := fiber.New()

	router.Get("/trades", app.fetchTradesHandler)

	router.Get("/trades/:ticker", app.getTradeHandler)

	if err := router.Listen(p); err != nil {
		return err
	}

	return nil
}
