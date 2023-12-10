package main

import (
	"fmt"
	"strconv"

	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/urfave/cli/v2"
)

var (
	//nolint:exhaustruct
	genkeysCommand = &cli.Command{
		Name: "genkeys",
		Usage: ("Generate a list of private keys for the crawler. " +
			"If FILE is not given, the keys will be printed to stdout"),
		ArgsUsage: "NUMBER_OF_KEYS [FILE]",
		Action:    genkeysAction,
	}
)

func genkeysAction(cCtx *cli.Context) error {
	args := cCtx.Args()

	if args.Len() == 0 {
		return fmt.Errorf("At least one positional argument is required: Number of keys to generate.")
	}
	if args.Len() > 2 {
		return fmt.Errorf("Too many positional arguments. Give at most two.")
	}

	nKeys, err := strconv.Atoi(args.Get(0))
	if err != nil {
		return fmt.Errorf("Parse number of keys parameter failed: %w.", err)
	}

	if nKeys == 0 {
		return fmt.Errorf("0 is not a valid number of keys to generate.")
	}

	// Default to write to stdout if FILE not given
	filename := "-"

	if args.Len() == 2 {
		filename = args.Get(1)
	}

	_, err = common.WriteNodeKeys(nKeys, filename)
	if err != nil {
		return fmt.Errorf("Write keys failed: %w.", err)
	}

	return nil
}
