// Copyright 2021 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"fmt"
	"net/http"
	"time"

	_ "modernc.org/sqlite"

	"github.com/oschwald/geoip2-golang"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/execution/crawler"
	"github.com/ethereum/node-crawler/pkg/execution/listener"

	"github.com/urfave/cli/v2"
)

var (
	//nolint:exhaustruct  // We don't need to specify everything.
	crawlerCommand = &cli.Command{
		Name:   "crawl",
		Usage:  "Crawl the ethereum network",
		Action: crawlNodesV2,
		Flags: []cli.Flag{
			&autovacuumFlag,
			&bootnodesFlag,
			&busyTimeoutFlag,
			&crawlerDBFlag,
			&discWorkersFlag,
			&geoipdbFlag,
			&githubTokenFileFlag,
			&listenAddrFlag,
			&listenStartPortFlag,
			&metricsAddressFlag,
			&nextCrawlFailFlag,
			&nextCrawlNotEthFlag,
			&nextCrawlSuccessFlag,
			&nodeFileFlag,
			&nodeKeysFileFlag,
			&nodeURLFlag,
			&nodedbFlag,
			&postgresFlag,
			&statsCopyFrequencyFlag,
			&statsDBFlag,
			&timeoutFlag,
			&workersFlag,
		},
	}
)

func openGeoIP(cCtx *cli.Context) (*geoip2.Reader, error) {
	geoipFile := geoipdbFlag.Get(cCtx)

	if geoipFile == "" {
		return nil, nil
	}

	geoipDB, err := geoip2.Open(geoipFile)
	if err != nil {
		return nil, fmt.Errorf("opening geoip database failed: %w", err)
	}

	return geoipDB, nil
}

func crawlNodesV2(cCtx *cli.Context) error {
	geoipDB, err := openGeoIP(cCtx)
	if err != nil {
		return fmt.Errorf("open geoip2 failed: %w", err)
	}

	if geoipDB != nil {
		defer geoipDB.Close()
	}

	db, err := openDBWriter(cCtx, geoipDB)
	if err != nil {
		return fmt.Errorf("open db failed: %w", err)
	}
	defer db.Close()

	// go db.TableStatsMetricsDaemon(cCtx.Context, 5*time.Minute)
	go db.CleanerDaemon(cCtx.Context, 3*time.Hour)
	go db.CopyStatsDaemon(statsCopyFrequencyFlag.Get(cCtx))

	nodeKeys, err := readNodeKeys(cCtx)
	if err != nil {
		return fmt.Errorf("node key: %w", err)
	}

	listener := listener.New(
		db,
		nodeKeys,
		listenAddrFlag.Get(cCtx),
		uint16(listenStartPortFlag.Get(cCtx)),
	)
	listener.StartDaemon(cCtx.Context)

	listener.StartDiscCrawlers(cCtx.Context, discWorkersFlag.Get(cCtx))

	crawler, err := crawler.New(
		db,
		nodeKeys,
	)
	if err != nil {
		return fmt.Errorf("create crawler: %w", err)
	}

	err = crawler.StartDaemon(
		cCtx.Context,
		workersFlag.Get(cCtx),
	)
	if err != nil {
		return fmt.Errorf("start crawler: %w", err)
	}

	// Start metrics server
	metricsAddr := metricsAddressFlag.Get(cCtx)
	log.Info("starting metrics server", "address", metricsAddr)
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(metricsAddr, nil)

	listener.Close()
	crawler.Close()

	listener.Wait()
	crawler.Wait()

	return nil
}
