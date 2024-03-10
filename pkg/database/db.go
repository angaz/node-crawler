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
)

type DB struct {
	db *sql.DB
	pg *pgxpool.Pool

	nextCrawlSucces time.Duration
	nextCrawlFail   time.Duration
	nextCrawlNotEth time.Duration
	githubToken     string

	executionNodesToCrawlCache chan *NodeToCrawl
	executionNodesToCrawlLock  *sync.Mutex
	executionRecentlyCrawled   *fifomemory.FIFOMemory[enode.ID]

	consensusNodesToCrawlCache chan *NodeToCrawl
	consensusNodesToCrawlLock  *sync.Mutex
	consensusRecentlyCrawled   *fifomemory.FIFOMemory[enode.ID]
	// consensusActiveCrawlers    map[enode.ID]struct{}

	discNodesToCrawlCache chan *NodeToCrawl
	discNodesToCrawlLock  *sync.Mutex
	discRecentlyCrawled   *fifomemory.FIFOMemory[enode.ID]

	portalDiscActiveCrawlers map[enode.ID]struct{}
	portalDiscToCrawlCache   chan *NodeToCrawl
	portalDiscToCrawlLock    *sync.Mutex
	portalRecentlyCrawled    *fifomemory.FIFOMemory[enode.ID]
}

func NewAPIDB(ctx context.Context, db *sql.DB, pgConnString string) (*DB, error) {
	return NewDB(ctx, db, pgConnString, 0, 0, 0, "")
}

func NewDB(
	ctx context.Context,
	db *sql.DB,
	pgConnString string,
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
		db: db,
		pg: pg,

		nextCrawlSucces: nextCrawlSucces,
		nextCrawlFail:   nextCrawlFail,
		nextCrawlNotEth: nextCrawlNotEth,
		githubToken:     githubToken,

		executionNodesToCrawlCache: make(chan *NodeToCrawl, 2048),
		executionNodesToCrawlLock:  new(sync.Mutex),
		executionRecentlyCrawled:   fifomemory.New[enode.ID](256),

		consensusNodesToCrawlCache: make(chan *NodeToCrawl, 2048),
		consensusNodesToCrawlLock:  new(sync.Mutex),
		consensusRecentlyCrawled:   fifomemory.New[enode.ID](256),
		// consensusActiveCrawlers:    map[enode.ID]struct{}{},

		discNodesToCrawlCache: make(chan *NodeToCrawl, 2048),
		discNodesToCrawlLock:  new(sync.Mutex),
		discRecentlyCrawled:   fifomemory.New[enode.ID](256),

		portalDiscActiveCrawlers: map[enode.ID]struct{}{},
		portalDiscToCrawlCache:   make(chan *NodeToCrawl, 2048),
		portalDiscToCrawlLock:    new(sync.Mutex),
		portalRecentlyCrawled:    fifomemory.New[enode.ID](128),
	}, nil
}

func (db *DB) Close() error {
	db.pg.Close()

	if db.db != nil {
		db.db.Close()
	}

	return nil
}

func (db *DB) tableStats(ctx context.Context) {
	var err error

	defer metrics.ObserveDBQuery("table_stats", time.Now(), err)

	row := db.pg.QueryRow(
		ctx,
		`
			SELECT
				(
					SELECT COUNT(*)
					FROM crawler.next_node_crawl
					LEFT JOIN crawler.next_disc_crawl USING (node_id)
					WHERE
						next_node_crawl.next_crawl < now()
						AND node_type IN ('Unknown', 'Execution')
						AND last_found > now() - INTERVAL '48 hours'
				),
				(
					SELECT COUNT(*)
					FROM crawler.next_disc_crawl
					WHERE
						next_crawl < now()
				)
		`,
	)

	var toCrawl, discToCrawl int64

	err = row.Scan(
		&toCrawl,
		&discToCrawl,
	)
	if err != nil {
		slog.Error("table stats scan failed", "err", err)

		return
	}

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
			FROM crawler.next_disc_crawl
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
