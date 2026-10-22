package main

import (
	"os"

	"objstore/internal/cli"
)

func main() {
	app := cli.New(os.Stdout, os.Stderr, os.Stdin)
	code := app.Run(os.Args[1:])
	os.Exit(code)
}
