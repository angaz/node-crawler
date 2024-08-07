//nolint:exhaustruct
package main

import (
	"time"

	"github.com/urfave/cli/v2"
)

var (
	apiListenAddrFlag = cli.StringFlag{
		Name:  "api-addr",
		Usage: "Listening address",
		Value: "0.0.0.0:10000",
	}
	autovacuumFlag = cli.StringFlag{
		Name: "autovacuum",
		Usage: ("Sets the autovacuum value for the databases. Possible values: " +
			"NONE, FULL, or INCREMENTAL. " +
			"https://www.sqlite.org/pragma.html#pragma_auto_vacuum"),
		Value: "INCREMENTAL",
	}
	snapshotDirFlag = cli.StringFlag{
		Name:  "snapshot-dir",
		Usage: "Snapshot directory.",
		Value: "snapshots",
	}
	bootnodesFlag = cli.StringSliceFlag{
		Name: "bootnodes",
		Usage: ("Comma separated nodes used for bootstrapping. " +
			"Defaults to hard-coded values for the selected network"),
	}
	busyTimeoutFlag = cli.Uint64Flag{
		Name: "busy-timeout",
		Usage: ("Sets the busy_timeout value for the database in milliseconds. " +
			"https://www.sqlite.org/pragma.html#pragma_busy_timeout"),
		Value: 3000,
	}
	crawlerDBFlag = cli.StringFlag{
		Name:  "crawler-db",
		Usage: "Crawler SQLite file name",
	}
	githubTokenFileFlag = cli.StringFlag{
		Name: "github-token",
		Usage: ("File containing a GitHub access token. Used for downloading " +
			"Ephemery releases."),
		Value: "github_token",
	}
	statsDBFlag = cli.StringFlag{
		Name:  "stats-db",
		Usage: "Stats SQLite file name",
	}
	dropNodesTimeFlag = cli.DurationFlag{
		Name:  "drop-time",
		Usage: "Time to drop crawled nodes without any updates",
		Value: 24 * time.Hour,
	}
	enodeAddrFlag = cli.StringFlag{
		Name:     "enode-addr",
		Usage:    "Enode address of the crawler",
		Required: true,
	}
	geoipdbFlag = cli.StringFlag{
		Name:  "geoipdb",
		Usage: "geoip2 database location",
	}
	listenAddrFlag = cli.StringFlag{
		Name:  "listen-addr",
		Usage: "Listening address",
		Value: "0.0.0.0",
	}
	listenStartPortFlag = cli.IntFlag{
		Name:  "listen-start-port",
		Usage: "Port to start listeners on",
		Value: 30303,
	}
	portalListenStartPortFlag = cli.IntFlag{
		Name:  "portal-listen-start-port",
		Usage: "Port to start listeners on",
		Value: 30320,
	}
	portalNodeKeysFileFlag = cli.StringFlag{
		Name:        "portal-keys",
		Category:    "portal",
		DefaultText: "",
		Value:       "portal.keys",
		TakesFile:   true,
	}
	nodedbFlag = cli.StringFlag{
		Name:  "nodedb",
		Usage: "Nodes database location. Defaults to in memory database",
	}
	nodeFileFlag = cli.StringFlag{
		Name:  "nodefile",
		Usage: "Path to a node file containing nodes to be crawled",
	}
	executionNodeKeysFileFlag = cli.StringFlag{
		Name:      "execution-nodekeys",
		Usage:     "Filename of the execution node keys",
		Value:     "execution.keys",
		TakesFile: true,
	}
	consensusNodeKeysFileFlag = cli.StringFlag{
		Name:      "consensus-nodekeys",
		Usage:     "Filename of the consensus node keys",
		Value:     "consensus.keys",
		TakesFile: true,
	}
	nodeURLFlag = cli.StringFlag{
		Name:  "nodeURL",
		Usage: "URL of the node you want to connect to",
		// Value: "http://localhost:8545",
	}
	postgresFlag = cli.StringFlag{
		Name:     "postgres",
		Usage:    "Postgresql connection string",
		Required: true,
	}
	timeoutFlag = cli.DurationFlag{
		Name:  "timeout",
		Usage: "Timeout for the crawling in a round",
		Value: 5 * time.Minute,
	}
	executionWorkersFlag = cli.IntFlag{
		Name:  "execution-workers",
		Usage: "Number of workers to start for updating nodes",
		Value: 16,
	}
	consensusWorkersFlag = cli.IntFlag{
		Name:  "consensus-workers",
		Usage: "Number of workers to start for crawling consensus nodes",
		Value: 16,
	}
	discWorkersFlag = cli.IntFlag{
		Name:  "disc-workers",
		Usage: "Number of discovery workers to start",
		Value: 1,
	}
	metricsAddressFlag = cli.StringFlag{
		Name:  "metrics-addr",
		Usage: "Address for the metrics server",
		Value: "0.0.0.0:9191",
	}
	statsUpdateFrequencyFlag = cli.DurationFlag{
		Name:  "stats-update",
		Usage: "Frequency at which the stats are updated",
		Value: 10 * time.Minute,
	}
	statsCopyFrequencyFlag = cli.DurationFlag{
		Name:  "stats-copy-frequency",
		Usage: "Frequency at which the stats should be copied to the stats DB",
		Value: 1 * time.Hour,
	}
	nextCrawlSuccessFlag = cli.DurationFlag{
		Name:  "next-crawl-success",
		Usage: "Next crawl value if the crawl was successful",
		Value: 12 * time.Hour,
	}
	nextCrawlFailFlag = cli.DurationFlag{
		Name:  "next-crawl-fail",
		Usage: "Next crawl value if the crawl was unsuccessful",
		Value: 48 * time.Hour,
	}
	nextCrawlNotEthFlag = cli.DurationFlag{
		Name:  "next-crawl-not-eth",
		Usage: "Next crawl value if the node was not an eth node",
		Value: 14 * 24 * time.Hour,
	}
)
