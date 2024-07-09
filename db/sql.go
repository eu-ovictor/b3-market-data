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
