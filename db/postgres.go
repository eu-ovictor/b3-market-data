package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgreSQL struct {
	pool *pgxpool.Pool
	uri  string
}

func (p *PostgreSQL) Close() { p.pool.Close() }

func (p *PostgreSQL) InsertMany(trades []Trade) error {
	batch := &pgx.Batch{}

	for _, trade := range trades {
		batch.Queue(CREATE_TRADE, trade.Ticker, trade.GrossAmount, trade.Quantity, trade.EntryTime, trade.Date)
	}

	result := p.pool.SendBatch(context.Background(), batch)
	defer result.Close()

	if result == nil {
		return errors.New("got empty result while creating trades")
	}

	return nil
}

func (p *PostgreSQL) FetchTrades(date string) ([]TradeSummary, error) {
	var (
		rows pgx.Rows
		err  error
	)

	if date != "" {
		rows, err = p.pool.Query(context.Background(), FETCH_BY_DATE, date)
	} else {
		rows, err = p.pool.Query(context.Background(), FETCH_ALL)
	}

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var trades []TradeSummary

	for rows.Next() {
		var trade TradeSummary

		err := rows.Scan(&trade.Ticker, &trade.MaxRangeValue, &trade.MaxDailyVolume)
		if err != nil {
			if err == pgx.ErrNoRows {
				return []TradeSummary{}, nil
			}

			return nil, err
		}

		trades = append(trades, trade)
	}

	return trades, nil
}

func (p *PostgreSQL) GetTrade(ticker string, date string) (TradeSummary, error) {
	var row pgx.Row

	if date != "" {
		row = p.pool.QueryRow(context.Background(), GET_BY_TICKER_AND_DATE, ticker, date)
	} else {
		row = p.pool.QueryRow(context.Background(), GET_BY_TICKER, ticker)
	}

	var trade TradeSummary

	err := row.Scan(&trade.Ticker, &trade.MaxRangeValue, &trade.MaxDailyVolume)
	if err != nil {
		if err == pgx.ErrNoRows {
			emptyTrade := TradeSummary{
				Ticker:         ticker,
				MaxRangeValue:  0,
				MaxDailyVolume: 0,
			}

			return emptyTrade, nil
		}

		return TradeSummary{}, err
	}

	return trade, nil
}

func (p *PostgreSQL) CreateTable() error {
	if _, err := p.pool.Exec(context.Background(), CREATE_TABLE); err != nil {
		return err
	}

	if _, err := p.pool.Exec(context.Background(), CREATE_HYPERTABLE); err != nil {
		return err
	}

	return nil
}

func (p *PostgreSQL) DropTable() error {
	if _, err := p.pool.Exec(context.Background(), DROP_MATERIALIZED_VIEW); err != nil {
		return err
	}

	if _, err := p.pool.Exec(context.Background(), DROP_TABLE); err != nil {
		return err
	}

	return nil
}

func (p *PostgreSQL) PostLoad() error {
	if _, err := p.pool.Exec(context.Background(), CREATE_MATERIALIZED_VIEW); err != nil {
		return err
	}

	if _, err := p.pool.Exec(context.Background(), CREATE_INDEXES); err != nil {
		return err
	}

	return nil
}

func NewPostgreSQL(uri string) (PostgreSQL, error) {
	cfg, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return PostgreSQL{}, fmt.Errorf("could not create database config: %w", err)
	}

	conn, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return PostgreSQL{}, fmt.Errorf("could not connect to the database: %w", err)
	}

	p := PostgreSQL{
		pool: conn,
		uri:  uri,
	}

	if err := p.pool.Ping(context.Background()); err != nil {
		return PostgreSQL{}, fmt.Errorf("could not connect to postgres: %w", err)
	}

	return p, nil
}
