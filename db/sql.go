package db

const FETCH_ALL = `
    SELECT 
      ticker, 
      MAX(max_range_value) AS max_range_value, 
      MAX(total_quantity) AS max_daily_volume 
    FROM 
      trade_summary 
    GROUP BY 
      ticker;
`

const FETCH_BY_DATE = ` 
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

const GET_BY_TICKER = ` 
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

const GET_BY_TICKER_AND_DATE = ` 
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
const CREATE_TRADE = `
    INSERT INTO trade (ticker, gross_amount, quantity, entry_time, date)
    VALUES ($1, $2, $3, $4, $5)
`

const CREATE_TABLE = `
    CREATE TABLE IF NOT EXISTS trade (
        ticker TEXT NOT NULL, 
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
    CREATE MATERIALIZED VIEW IF NOT EXISTS trade_summary 
    AS
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

const DROP_TABLE = `
    DROP TABLE trade;
`

const DROP_MATERIALIZED_VIEW = `
    DROP MATERIALIZED VIEW trade_summary;
`
