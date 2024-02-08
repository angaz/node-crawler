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
	executioncrawler "github.com/ethereum/node-crawler/pkg/execution/crawler"
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
			&consensusNodeKeysFileFlag,
			&consensusWorkersFlag,
			&crawlerDBFlag,
			&discWorkersFlag,
			&executionNodeKeysFileFlag,
			&executionWorkersFlag,
			&geoipdbFlag,
			&githubTokenFileFlag,
			&listenAddrFlag,
			&listenStartPortFlag,
			&metricsAddressFlag,
			&nextCrawlFailFlag,
			&nextCrawlNotEthFlag,
			&nextCrawlSuccessFlag,
			&nodeFileFlag,
			&nodeURLFlag,
			&nodedbFlag,
			&portalListenStartPortFlag,
			&portalNodeKeysFileFlag,
			&postgresFlag,
			&statsCopyFrequencyFlag,
			&statsDBFlag,
			&timeoutFlag,
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

	executionNodeKeys, err := common.ReadNodeKeys(executionNodeKeysFileFlag.Get(cCtx))
	if err != nil {
		return fmt.Errorf("read execution node keys: %w", err)
	}

	// consensusNodeKeys, err := common.ReadNodeKeys(consensusNodeKeysFileFlag.Get(cCtx))
	// if err != nil {
	// 	return fmt.Errorf("read consensus node keys: %w", err)
	// }

	portalKeys, err := common.ReadNodeKeys(portalNodeKeysFileFlag.Get(cCtx))
	if err != nil {
		return fmt.Errorf("read portal node keys: %w", err)
	}

	listener := listener.New(
		db,
		executionNodeKeys,
		listenAddrFlag.Get(cCtx),
		uint16(listenStartPortFlag.Get(cCtx)),
	)
	listener.StartDaemon(cCtx.Context)

	listener.StartDiscCrawlers(cCtx.Context, discWorkersFlag.Get(cCtx))

	execCrawler, err := executioncrawler.New(
		db,
		executionNodeKeys,
	)
	if err != nil {
		return fmt.Errorf("create execution crawler: %w", err)
	}

	err = execCrawler.StartDaemon(
		cCtx.Context,
		executionWorkersFlag.Get(cCtx),
	)
	if err != nil {
		return fmt.Errorf("start execution crawler: %w", err)
	}

	// consensusCrawler, err := consensuscrawler.New(
	// 	db,
	// 	consensusNodeKeys,
	// )
	// if err != nil {
	// 	return fmt.Errorf("create consensus crawler: %w", err)
	// }

	// err = consensusCrawler.StartDeamon(
	// 	cCtx.Context,
	// 	consensusWorkersFlag.Get(cCtx),
	// )
	// if err != nil {
	// 	return fmt.Errorf("start consensus crawler: %w", err)
	// }

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
	execCrawler.Close()
	// consensusCrawler.Close()
	portalListener.Close()

	listener.Wait()
	execCrawler.Wait()
	// consensusCrawler.Wait()
	portalListener.Wait()

	return nil
}
