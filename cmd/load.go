package cmd

import (
	"github.com/eu-ovictor/b3-market-data/db"
	"github.com/eu-ovictor/b3-market-data/loader"
	"github.com/spf13/cobra"
)

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

		if err := pg.CreateTable(); err != nil {
			return err
		}

		if err := loader.Load(dir, batchSize, &pg); err != nil {
			return err
		}

		if err := pg.PostLoad(); err != nil {
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
