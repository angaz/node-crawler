package main

import "github.com/urfave/cli/v2"

var (
	clientInfoCommand = &cli.Command{
		Name:   "client-info",
		Usage:  "Connect to a client and get it's info, similar to what the crawler will do.",
		Action: clientInfoRun,
		Flags:  []cli.Flag{},
	}
)

func clientInfoRun(ctx *cli.Context) error {
	return nil
}
