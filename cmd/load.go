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
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/schollz/progressbar/v3"
)

// TODO: read from env
const DATABASE_URL = "postgres://root:passwd@localhost:5432/b3-market-data?pool_max_conns=20"
const ASSETS_PATH = "./sample"
const BATCH_SIZE = 100000

const CREATE_TABLE = `
    CREATE TABLE IF NOT EXISTS trade (
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


func processRow(row []string) (Trade, error) {
    ticker := row[1]

    grossAmount, err := parseGrossAmount(row[3])
    if err != nil {
        return Trade{}, err
    }

    quantity, err := strconv.ParseInt(row[4], 10, 64)
    if err != nil {
        return Trade{}, err
    }

    entryTime, err := parseEntryTime(row[5])
    if err != nil {
        return Trade{}, err
    }

    layout := "2006-01-02"
    date, err := time.Parse(layout, row[8])
    if err != nil {
        return Trade{}, err
    }

    return Trade{
        Ticker: ticker,
        GrossAmount: grossAmount,
        Quantity: quantity,
        EntryTime: entryTime,
        Date: date,
    }, nil
}

func processFile(wg *sync.WaitGroup, file os.DirEntry, pool *pgxpool.Pool, pbar *progressbar.ProgressBar){
    defer wg.Done()

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

    batch := &pgx.Batch{}

    for {
        row, err := r.Read()
        if err == io.EOF {
            // process remaining rows
            result := pool.SendBatch(context.Background(), batch)
            if result == nil {
                fmt.Fprintln(os.Stderr, fmt.Errorf("got empty batch result while processing file %s", file.Name()))
                os.Exit(1)
            }
            result.Close()

            pbar.Add(batch.Len())

            break
        }
        if err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }

        trade, err := processRow(row)
        if err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }

        batch.Queue(INSERT_INTO, trade.Ticker, trade.GrossAmount, trade.Quantity, trade.EntryTime, trade.Date)

        if batch.Len() == BATCH_SIZE {
            wg.Add(1)

            go func(wg *sync.WaitGroup, pool *pgxpool.Pool, batch *pgx.Batch) {
                defer wg.Done()
                result := pool.SendBatch(context.Background(), batch)
                if result == nil {
                    fmt.Fprintln(os.Stderr, fmt.Errorf("got empty batch result while processing file %s", file.Name()))
                    os.Exit(1)
                }
                result.Close()
            }(wg, pool, batch)

            pbar.Add(batch.Len())
            batch = &pgx.Batch{}
        }
    }
}

func main() {
    pbar := progressbar.Default(-1, "rows inserted")
    defer pbar.Close()

    pool, err := pgxpool.New(context.Background(), DATABASE_URL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer pool.Close()

	files, err := os.ReadDir(ASSETS_PATH)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if _, err := pool.Exec(context.Background(), CREATE_TABLE); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

    var wg sync.WaitGroup

	for _, file := range files {
        wg.Add(1)

        go processFile(&wg, file, pool, pbar)
	}

    wg.Wait()
}
