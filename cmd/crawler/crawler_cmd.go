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

	"log/slog"

	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/execution/crawler"
	"github.com/ethereum/node-crawler/pkg/execution/listener"
	portallistener "github.com/ethereum/node-crawler/pkg/portal/listener"

	"github.com/urfave/cli/v2"
)

var (
	//nolint:exhaustruct  // We don't need to specify everything.
	crawlerCommand = &cli.Command{
		Name:   "crawl",
		Usage:  "Crawl the ethereum network",
		Action: crawlerAction,
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
			&portalKeys,
			&portalListenStartPortFlag,
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

func crawlerAction(cCtx *cli.Context) error {
	db, err := openDBWriter(cCtx)
	if err != nil {
		return fmt.Errorf("open db failed: %w", err)
	}
	defer db.Close()

	err = db.Migrate(geoipdbFlag.Get(cCtx))
	if err != nil {
		return fmt.Errorf("database migration failed: %w", err)
	}

	go db.CleanerDaemon(cCtx.Context, 3*time.Hour)
	go db.CopyPortalStatsDaemon(cCtx.Context, statsCopyFrequencyFlag.Get(cCtx))
	go db.CopyStatsDaemon(statsCopyFrequencyFlag.Get(cCtx))
	go db.EphemeryInsertDaemon(cCtx.Context, time.Hour)
	go db.MissingBlocksDaemon(cCtx.Context, 5*time.Minute)
	go db.TableStatsMetricsDaemon(cCtx.Context, 5*time.Minute)
	go db.UpdateGeoIPDaemon(cCtx.Context, time.Hour, geoipdbFlag.Get(cCtx))

	nodeKeys, err := readNodeKeys(cCtx)
	if err != nil {
		return fmt.Errorf("node key: %w", err)
	}

	portalKeys, err := common.ReadNodeKeys(portalKeys.Get(cCtx))
	if err != nil {
		return fmt.Errorf("read portal keys: %w", err)
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

	portalListener := portallistener.New(
		db,
		portalKeys,
		listenAddrFlag.Get(cCtx),
		uint16(portalListenStartPortFlag.Get(cCtx)),
	)
	portalListener.StartDaemon(cCtx.Context)
	portalListener.StartDiscCrawlers(cCtx.Context, 1)

	// Start metrics server
	metricsAddr := metricsAddressFlag.Get(cCtx)
	slog.Info("starting metrics server", "address", metricsAddr)
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(metricsAddr, nil)

	listener.Close()
	crawler.Close()
	portalListener.Close()

	listener.Wait()
	crawler.Wait()
	portalListener.Wait()

	return nil
}
