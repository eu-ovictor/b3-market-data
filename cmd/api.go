package cmd

import (
	"fmt"
	"os"


	"github.com/eu-ovictor/b3-market-data/api"
	"github.com/eu-ovictor/b3-market-data/db"
	"github.com/spf13/cobra"
)

const defaultPort = "8000"

var port string 

var apiCmd = &cobra.Command {
    Use: "api",
    Short: "Spins up the web API",
    RunE: func(_ *cobra.Command, _ []string) error {
        u, err := loadDatabaseURI()
        if err != nil {
            return err 
        }

        if port == "" {
			port = os.Getenv("PORT")
		}

		if port == "" {
			port = defaultPort
		}

        pg, err := db.NewPostgreSQL(u) 
        if err != nil {
            return err 
        }
        defer pg.Close()

        api.Serve(&pg, port)

        return nil
    },
}

func apiCLI() *cobra.Command {
    apiCmd.Flags().StringVarP(&port, "port", "p", defaultPort, fmt.Sprintf("web server port (default PORT environment variable or %s)", defaultPort))

    return apiCmd
}
