package cmd

import (
	"archive/zip"
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
	"github.com/spf13/cobra"
)

const CREATE_TABLE = `
    CREATE TABLE IF NOT EXISTS trade (
        ticker VARCHAR(10) NOT NULL, 
        gross_amount NUMERIC(10, 3),
        quantity INT NOT NULL,
        entry_time TIME WITH TIME ZONE, 
        date DATE
    );
`
const CREATE_HYPERTABLE = `
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'trade' AND table_schema = 'public') THEN
            SELECT create_hypertable('trade', 'date');
        END IF;
    END $$;
`
const CREATE_MATERIALIZED_VIEW = `
    CREATE MATERIALIZED VIEW IF NOT EXISTS trade_summary AS
    SELECT
        time_bucket('1 day', date) AS date,
        ticker,
        MAX(gross_amount) AS max_range_value,
        SUM(quantity) AS total_quantity
    FROM
        trade
    GROUP BY
        date, ticker;
`
const CREATE_INDEXES = `
    CREATE INDEX IF NOT EXISTS idx_trade_summary_ticker_day ON trade_summary (ticker, date);
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

func processFile(
    filePath string,
    s int,
    pool *pgxpool.Pool, 
    pbar *progressbar.ProgressBar,
) error {
    var wg sync.WaitGroup

    zipFile, err := zip.OpenReader(filePath)
    if err != nil {
        return err
    }
    defer zipFile.Close()


    var tradesFile *zip.File 

    for _, f := range zipFile.File {
        if filepath.Ext(f.Name) == ".txt" {
            tradesFile = f 
            break
        }
    }

    if tradesFile == nil {
        return nil 
    }

    f, err := tradesFile.Open()
    if err != nil {
        return err
    }
    defer f.Close()

    r := csv.NewReader(f)
    r.Comma = ';'

    // ignore header
    _, err = r.Read()
    if err != nil {
        return err
    }

    batch := &pgx.Batch{}

    for {
        row, err := r.Read()
        if err == io.EOF {
            // process remaining rows
            result := pool.SendBatch(context.Background(), batch)
            if result == nil {
                return fmt.Errorf("got empty batch result while processing file %s", filePath)
            }
            result.Close()

            pbar.Add(batch.Len())

            break
        }
        if err != nil {
            return err
        }

        trade, err := processRow(row)
        if err != nil {
            return err
        }

        batch.Queue(INSERT_INTO, trade.Ticker, trade.GrossAmount, trade.Quantity, trade.EntryTime, trade.Date)

        if batch.Len() == s {
            wg.Add(1)

            go func(wg *sync.WaitGroup, pool *pgxpool.Pool, batch *pgx.Batch) {
                defer wg.Done()
                result := pool.SendBatch(context.Background(), batch)
                result.Close()
            }(&wg, pool, batch)

            pbar.Add(batch.Len())
            batch = &pgx.Batch{}
        }
    }

    wg.Wait()
    
    return nil
}

func loadB3MarketData(dir string, u string, s int) error {
    pbar := progressbar.Default(-1, "rows inserted")
    defer pbar.Close()

    pool, err := pgxpool.New(context.Background(), u)
	if err != nil {
        return err
	}
	defer pool.Close()

	files, err := os.ReadDir(dir)
	if err != nil {
        return err
	}

	if _, err := pool.Exec(context.Background(), CREATE_TABLE); err != nil {
        return err
	}

	if _, err := pool.Exec(context.Background(), CREATE_HYPERTABLE); err != nil {
        return err
	}

	for _, file := range files {
        filePath := filepath.Join(dir, file.Name())

        if err := processFile(filePath, s, pool, pbar); err != nil {
            return err
        }
	}

	if _, err := pool.Exec(context.Background(), CREATE_MATERIALIZED_VIEW); err != nil {
        return err

	}

	if _, err := pool.Exec(context.Background(), CREATE_INDEXES); err != nil {
        return err
	}

    return nil
}

var batchSize int


var loadCmd = &cobra.Command{
    Use: "load",
    Short: "Loads downloaded B3 market data into database.",
    RunE: func(_ *cobra.Command, _ []string) error {
        if err := assertDirExists(); err != nil {
			return err
        }

        u, err := loadDatabaseURI()
        if err != nil {
            return err 
        }

        return loadB3MarketData(dir, u, batchSize)
    },
}

func loadCLI() *cobra.Command {
    loadCmd = addDataDir(loadCmd)
    loadCmd.Flags().IntVarP(&batchSize, "batch-size", "b", 1000, "max length of rows inserted at once")
    return loadCmd
}

