package main

import (
	"os"

	"github.com/urfave/cli"
)

// Version is our app version and should be set by ldflags.
var Version string

func main() {

	app := cli.NewApp()
	app.Version = Version

	var confFlag = cli.StringFlag{
		Name:  "conf",
		Usage: "path to config file",
		Value: "config.yml",
	}

	// Global Flags
	app.Flags = []cli.Flag{confFlag}

	app.Run(os.Args)
}
