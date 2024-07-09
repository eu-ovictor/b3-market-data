package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const (
	defaultDataDir = "data"
	help           = `B3 Market data.

Toolbox to manage B3 Market data, including tools to handle load data and spin up the web server.

See --help for more details.
`
)

var (
	dir         string
	databaseURI string
)

func addDataDir(c *cobra.Command) *cobra.Command {
	c.Flags().StringVarP(&dir, "directory", "d", defaultDataDir, "directory of the downloaded files")
	return c
}

func addDatabase(c *cobra.Command) *cobra.Command {
	c.Flags().StringVarP(&databaseURI, "database-uri", "u", "", "PostgreSQL URI (default DATABASE_URL environment variable)")
	return c
}

func assertDirExists() error {
	i, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return fmt.Errorf("directory %s does not exist", dir)
	}
	if err != nil {
		return err
	}
	if !i.Mode().IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	return nil
}

func loadDatabaseURI() (string, error) {
	if databaseURI != "" {
		return databaseURI, nil
	}

	u := os.Getenv("DATABASE_URL")

	if u == "" {
		return "", fmt.Errorf("could not find a database URI, pass it as a flag or set DATABASE_URL environment variable with the credentials for a PostgreSQL database")
	}

	return u, nil
}

var rootCmd = &cobra.Command{
	Use:   "b3-market-data <command>",
	Short: "B3 Market data toolbox",
	Long:  help,
}

// CLI returns the root command from Cobra CLI tool.
func CLI() *cobra.Command {
	for _, c := range []*cobra.Command{apiCLI(), loadCLI()} {
		addDatabase(c)
		rootCmd.AddCommand(c)
	}

	return rootCmd
}
