package cmd

import (
	"context"
	"fmt"

	"github.com/eu-ovictor/b3-market-data/db"
	"github.com/eu-ovictor/b3-market-data/loader"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

func createTables(u string) error {
	ctx := context.Background()

	cfg, err := pgx.ParseConfig(u)
	if err != nil {
		return fmt.Errorf("could not create database config: %w", err)
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(context.Background(), db.CREATE_TABLE); err != nil {
		return err
	}

	if _, err := conn.Exec(context.Background(), db.CREATE_HYPERTABLE); err != nil {
		return err
	}

	return nil
}

func createViews(u string) error {
	ctx := context.Background()

	cfg, err := pgx.ParseConfig(u)
	if err != nil {
		return fmt.Errorf("could not create database config: %w", err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(context.Background(), db.CREATE_MATERIALIZED_VIEW); err != nil {
		return err
	}

	if _, err := conn.Exec(context.Background(), db.CREATE_INDEXES); err != nil {
		return err
	}

	return nil
}

var batchSize int

var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "Loads downloaded B3 market data into database.",
	RunE: func(_ *cobra.Command, _ []string) error {
		if err := assertDirExists(); err != nil {
			return err
		}

		u, err := loadDatabaseURI()
		if err != nil {
			return err
		}

		pg, err := db.NewPostgreSQL(u)
		if err != nil {
			return err
		}
		defer pg.Close()

		if err := createTables(u); err != nil {
			return err
		}

		if err := loader.Load(dir, batchSize, &pg); err != nil {
			return err
		}

		if err := createViews(u); err != nil {
			return err
		}

		return nil
	},
}

func loadCLI() *cobra.Command {
	loadCmd = addDataDir(loadCmd)
	loadCmd.Flags().IntVarP(&batchSize, "batch-size", "b", 1000, "max length of rows inserted at once")
	return loadCmd
}
