package main

import (
	"fmt"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/node-crawler/pkg/crawler"
	"github.com/urfave/cli/v2"
)

var (
	clientInfoCommand = &cli.Command{
		Name:      "client-info",
		ArgsUsage: "<node>",
		Usage:     "Connect to a client and get client info",
		Action:    runClientInfo,
		Flags: []cli.Flag{
			&nodekeyFlag,
			utils.GoerliFlag,
			utils.HoleskyFlag,
			utils.NetworkIdFlag,
			utils.SepoliaFlag,
		},
	}
)

func runClientInfo(cCtx *cli.Context) error {
	if cCtx.NArg() < 1 {
		return fmt.Errorf("missing node positional argument")
	}
	if cCtx.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	node, err := crawler.ParseNode(cCtx.Args().First())
	if err != nil {
		return fmt.Errorf("parsing node failed: %w", err)
	}

	genesis := makeGenesis(cCtx)
	networkID := cCtx.Uint64(utils.NetworkIdFlag.Name)

	clientInfo, err := crawler.GetClientInfo(genesis, networkID, "", node)
	if err != nil {
		return fmt.Errorf("getClientInfo failed: %w", err)
	}

	fmt.Printf("ClientInfo:\n%#v\n", clientInfo)

	return nil
}

func makeGenesis(cCtx *cli.Context) *core.Genesis {
	if cCtx.Bool(utils.HoleskyFlag.Name) {
		return core.DefaultHoleskyGenesisBlock()
	}
	if cCtx.Bool(utils.SepoliaFlag.Name) {
		return core.DefaultSepoliaGenesisBlock()
	}
	if cCtx.Bool(utils.GoerliFlag.Name) {
		return core.DefaultGoerliGenesisBlock()
	}

	return core.DefaultGenesisBlock()
}
