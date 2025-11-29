package main

import (
	"fmt"
	"strconv"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/urfave/cli/v2"
)

var (
	//nolint:exhaustruct
	printEnodesCommand = &cli.Command{
		Name:      "print-enodes",
		Usage:     "Print the enodes from the given node keys file",
		ArgsUsage: "KEYS_FILE HOST START_PORT",
		Action:    printEnodesAction,
	}
)

func printEnodesAction(cCtx *cli.Context) error {
	args := cCtx.Args()

	if args.Len() != 3 {
		return fmt.Errorf("3 positional arguments required")
	}

	nodeFile := args.Get(0)
	hostname := args.Get(1)

	startPort, err := strconv.Atoi(args.Get(2))
	if err != nil {
		return fmt.Errorf("parsing start port failed: %w", err)
	}

	nodeKeys, err := common.ReadNodeKeys(nodeFile)
	if err != nil {
		return fmt.Errorf("reading node keys file failed: %w", err)
	}

	for i, nodeKey := range nodeKeys {
		fmt.Printf(
			"enode://%x@%s:%d\n",
			crypto.FromECDSAPub(&nodeKey.PublicKey)[1:],
			hostname,
			startPort+i,
		)
	}

	return nil
}
