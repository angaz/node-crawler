package main

import (
	"fmt"
	"net/http"
	"sync"

	_ "modernc.org/sqlite"

	"log/slog"

	"github.com/ethereum/node-crawler/pkg/api"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
)

//nolint:exhaustruct
var apiCommand = &cli.Command{
	Name:   "api",
	Usage:  "API server for the crawler",
	Action: runAPI,
	Flags: []cli.Flag{
		&apiListenAddrFlag,
		&busyTimeoutFlag,
		&dropNodesTimeFlag,
		&enodeAddrFlag,
		&listenStartPortFlag,
		&metricsAddressFlag,
		&executionNodeKeysFileFlag,
		&postgresFlag,
		&snapshotDirFlag,
		&statsUpdateFrequencyFlag,
	},
}

func runAPI(cCtx *cli.Context) error {
	db, err := openDBReader(cCtx)
	if err != nil {
		return fmt.Errorf("open db failed: %w", err)
	}
	defer db.Close()

	wg := new(sync.WaitGroup)
	wg.Add(1)

	enodes, err := readEnodes(cCtx)
	if err != nil {
		return fmt.Errorf("Read enodes failed: %w", err)
	}

	// Start the API deamon
	api := api.New(
		db,
		statsUpdateFrequencyFlag.Get(cCtx),
		enodes,
		snapshotDirFlag.Get(cCtx),
	)
	go api.StartServer(
		wg,
		apiListenAddrFlag.Get(cCtx),
	)

	// Start metrics server
	metricsAddr := metricsAddressFlag.Get(cCtx)
	slog.Info("starting metrics server", "address", metricsAddr)
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(metricsAddr, nil)

	wg.Wait()

	return nil
}

func readEnodes(cCtx *cli.Context) ([]string, error) {
	keys, err := common.ReadNodeKeys(executionNodeKeysFileFlag.Get(cCtx))
	if err != nil {
		return nil, fmt.Errorf("read node keys: %w", err)
	}

	enodes := common.KeysToEnodes(
		keys,
		enodeAddrFlag.Get(cCtx),
		listenStartPortFlag.Get(cCtx),
	)

	return enodes, nil
}
