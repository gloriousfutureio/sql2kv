package main

import (
	"fmt"
	"log"
	"os"

	"github.com/gloriousfutureio/sql2kv"
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

	app.Action = func(clictx *cli.Context) {
		conf, err := sql2kv.NewConfig(clictx.String("conf"))
		if err != nil {
			log.Fatalf("config error: %v", err)
		}
		fmt.Println("CONFIG", conf)
	}

	app.Run(os.Args)
}
