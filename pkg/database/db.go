package database

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strconv"
	"sync"
	"time"

	"log/slog"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/database/migrations"
	"github.com/ethereum/node-crawler/pkg/fifomemory"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oschwald/geoip2-golang"
)

type DB struct {
	db      *sql.DB
	pg      *pgxpool.Pool
	geoipDB *geoip2.Reader

	nextCrawlSucces time.Duration
	nextCrawlFail   time.Duration
	nextCrawlNotEth time.Duration
	githubToken     string

	nodesToCrawlCache chan *NodeToCrawl
	nodesToCrawlLock  *sync.Mutex
	recentlyCrawled   *fifomemory.FIFOMemory[enode.ID]

	discNodesToCrawlCache chan *NodeToCrawl
	discNodesToCrawlLock  *sync.Mutex
	discRecentlyCrawled   *fifomemory.FIFOMemory[enode.ID]

	discUpdateCache *fifomemory.FIFOMemory[enode.ID]
}

func NewAPIDB(ctx context.Context, db *sql.DB, pgConnString string) (*DB, error) {
	return NewDB(ctx, db, pgConnString, nil, 0, 0, 0, "")
}

func NewDB(
	ctx context.Context,
	db *sql.DB,
	pgConnString string,
	geoipDB *geoip2.Reader,
	nextCrawlSucces time.Duration,
	nextCrawlFail time.Duration,
	nextCrawlNotEth time.Duration,
	githubToken string,
) (*DB, error) {
	pg, err := pgxpool.New(ctx, pgConnString)
	if err != nil {
		return nil, fmt.Errorf("tmp conn: %w", err)
	}

	return &DB{
		db:      db,
		pg:      pg,
		geoipDB: geoipDB,

		nextCrawlSucces: nextCrawlSucces,
		nextCrawlFail:   nextCrawlFail,
		nextCrawlNotEth: nextCrawlNotEth,
		githubToken:     githubToken,

		nodesToCrawlCache: make(chan *NodeToCrawl, 16384),
		nodesToCrawlLock:  new(sync.Mutex),
		recentlyCrawled:   fifomemory.New[enode.ID](256),

		discNodesToCrawlCache: make(chan *NodeToCrawl, 16384),
		discNodesToCrawlLock:  new(sync.Mutex),
		discRecentlyCrawled:   fifomemory.New[enode.ID](1024),

		discUpdateCache: fifomemory.New[enode.ID](512),
	}, nil
}

func (db *DB) Close() error {
	db.pg.Close()

	if db.db != nil {
		db.db.Close()
	}

	return nil
}

type tableStats struct {
	totalDiscoveredNodes int64
	totalCrawledNodes    int64
	totalBlocks          int64
	totalToCrawl         int64
}

func (db *DB) tableStats(ctx context.Context) {
	var err error

	defer metrics.ObserveDBQuery("table_stats", time.Now(), err)

	row := db.pg.QueryRow(
		ctx,
		`
			SELECT
				(SELECT COUNT(*) FROM disc.nodes),
				(
					SELECT COUNT(*) FROM disc.nodes
					WHERE
						next_crawl < now()
						AND node_type IN ('Unknown', 'Execution')
				),
				(
					SELECT COUNT(*) FROM disc.nodes
					WHERE
						next_disc_crawl < now()
				),
				(SELECT COUNT(*) FROM execution.nodes),
				(SELECT COUNT(*) FROM execution.blocks)
		`,
	)

	var discoveredNodes, toCrawl, discToCrawl, crawledNodes, blocks int64

	err = row.Scan(
		&discoveredNodes,
		&toCrawl,
		&discToCrawl,
		&crawledNodes,
		&blocks,
	)
	if err != nil {
		slog.Error("table stats scan failed", "err", err)

		return
	}

	metrics.DBStatsBlocks.Set(float64(blocks))
	metrics.DBStatsCrawledNodes.Set(float64(crawledNodes))
	metrics.DBStatsDiscNodes.Set(float64(discoveredNodes))
	metrics.DBStatsNodesToCrawl.Set(float64(toCrawl))
	metrics.DBStatsDiscNodesToCrawl.Set(float64(discToCrawl))
}

const (
	hourSeconds = 3600
	daySeconds  = 24 * hourSeconds
)

var lastFoundSeconds = [13]int64{
	1 * hourSeconds,
	2 * hourSeconds,
	3 * hourSeconds,
	6 * hourSeconds,
	12 * hourSeconds,
	18 * hourSeconds,
	1 * daySeconds,
	2 * daySeconds,
	3 * daySeconds,
	4 * daySeconds,
	5 * daySeconds,
	6 * daySeconds,
	7 * daySeconds,
}

func (db *DB) lastFoundStats(ctx context.Context) {
	var err error

	defer metrics.ObserveDBQuery("last_found_stats", time.Now(), err)

	row := db.pg.QueryRow(
		ctx,
		`
			SELECT
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '1 hour'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '2 hour'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '3 hours'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '6 hours'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '12 hours'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '18 hours'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '1 day'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '2 day'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '3 days'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '4 days'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '5 days'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '6 days'),
				COUNT(*) FILTER (WHERE last_found > now() - INTERVAL '7 days')
			FROM disc.nodes
			WHERE last_found > now() - INTERVAL '7 days';
		`,
	)

	var counts [13]int64

	err = row.Scan(
		&counts[0],
		&counts[1],
		&counts[2],
		&counts[3],
		&counts[4],
		&counts[5],
		&counts[6],
		&counts[7],
		&counts[8],
		&counts[9],
		&counts[10],
		&counts[11],
		&counts[12],
	)
	if err != nil {
		slog.Error("get last found stats rows failed", "err", err)

		return
	}

	for i, count := range counts {
		metrics.LastFound.
			WithLabelValues(strconv.FormatInt(lastFoundSeconds[i], 10)).
			Set(float64(count))
	}
}

// Meant to be run as a goroutine
//
// Periodically collects the table stat metrics
func (db *DB) TableStatsMetricsDaemon(ctx context.Context, frequency time.Duration) {
	for ctx.Err() == nil {
		next := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(next))

		db.tableStats(ctx)
		db.lastFoundStats(ctx)
	}
}

type TxFn func(context.Context, pgx.Tx) error

var (
	//nolint:exhaustruct
	TxOptionsDefault = pgx.TxOptions{}
	//nolint:exhaustruct
	TxOptionsDeferrable = pgx.TxOptions{DeferrableMode: pgx.Deferrable}
)

func asyncCommitTxFn(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SET LOCAL synchronous_commit TO OFF")
	if err != nil {
		return fmt.Errorf("set async commit: %w", err)
	}

	return nil
}

func (db *DB) withTx(
	ctx context.Context,
	txOptions pgx.TxOptions,
	postBeginFn TxFn,
	fn TxFn,
	preCommitFn TxFn,
) error {
	tx, err := db.pg.BeginTx(ctx, txOptions)
	if err != nil {
		return fmt.Errorf("start tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if postBeginFn != nil {
		err = postBeginFn(ctx, tx)
		if err != nil {
			return fmt.Errorf("post-begin fn: %w", err)
		}
	}

	err = fn(ctx, tx)
	if err != nil {
		return fmt.Errorf("fn: %w", err)
	}

	if preCommitFn != nil {
		err = preCommitFn(ctx, tx)
		if err != nil {
			return fmt.Errorf("pre-commit fn: %w", err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

func (db *DB) WithTxAsync(
	ctx context.Context,
	txOptions pgx.TxOptions,
	fn TxFn,
) error {
	return db.withTx(
		ctx,
		txOptions,
		asyncCommitTxFn,
		fn,
		nil,
	)
}

func (db *DB) WithTx(ctx context.Context, fn TxFn) error {
	return db.withTx(
		ctx,
		TxOptionsDefault,
		nil,
		fn,
		nil,
	)
}

func (db *DB) EphemeryInsertDaemon(ctx context.Context, frequency time.Duration) {
	for ctx.Err() == nil {
		next := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(next))

		err := db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			err := migrations.InsertNewEphemeryNetworks(ctx, tx, db.githubToken)
			if err != nil {
				return fmt.Errorf("new ephemery: %w", err)
			}

			return nil
		})
		if err != nil {
			slog.Error("ephemery daemon failed", "err", err)
		}
	}
}
