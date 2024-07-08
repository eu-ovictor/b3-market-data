package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// TODO: read from env
const DATABASE_URL = "postgres://root:passwd@localhost:5432/b3-market-data"
const ASSETS_PATH = "./sample"

const CREATE_TABLE = `
    CREATE TABLE IF NOT EXISTS trade (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL, 
        gross_amount NUMERIC(10, 3),
        quantity INT NOT NULL,
        entry_time TIME WITH TIME ZONE, 
        date DATE
    );
`

const INSERT_INTO = `
    INSERT INTO trade (ticker, gross_amount, quantity, entry_time, date)
    VALUES ($1, $2, $3, $4, $5)
`

type Trade struct {
	Ticker      string
	GrossAmount float64
	Quantity    int64
	EntryTime   time.Time
	Date        time.Time
}

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

	return entryTime.UTC(), nil
}

func parseGrossAmount(s string) (float64, error) {
	s = strings.Replace(s, ",", ".", 1)

	return strconv.ParseFloat(s, 64)
}

func main() {
	conn, err := pgx.Connect(context.Background(), DATABASE_URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	files, err := os.ReadDir(ASSETS_PATH)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if _, err := conn.Exec(context.Background(), CREATE_TABLE); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for _, file := range files {
		filePath := filepath.Join(ASSETS_PATH, file.Name())

		f, err := os.Open(filePath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()

		r := csv.NewReader(f)
		r.Comma = ';'

		// ignore header
		_, err = r.Read()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			grossAmount, err := parseGrossAmount(rec[3])
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			quantity, err := strconv.ParseInt(rec[4], 10, 64)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			layout := "2006-01-02"
			date, err := time.Parse(layout, rec[8])
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			entryTime, err := parseEntryTime(rec[5])
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			trade := Trade{
				Ticker:      rec[1],
				GrossAmount: grossAmount,
				Quantity:    quantity,
				EntryTime:   entryTime,
				Date:        date,
			}

			if _, err := conn.Exec(
                context.Background(), 
                INSERT_INTO, 
                trade.Ticker, 
                trade.GrossAmount, 
                trade.Quantity, 
                trade.EntryTime, 
                trade.Date,
            ); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}

			fmt.Printf("%v\n", trade)
		}
	}
}
