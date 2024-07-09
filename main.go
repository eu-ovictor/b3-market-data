package main

import (
	"fmt"
	"os"

	"github.com/eu-ovictor/b3-market-data/cmd"
)

func main() {
	if err := cmd.CLI().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
