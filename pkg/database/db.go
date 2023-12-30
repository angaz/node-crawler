package database

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
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
		discRecentlyCrawled:   fifomemory.New[enode.ID](256),
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
		log.Error("table stats scan failed", "err", err)

		return
	}

	metrics.DBStatsBlocks.Set(float64(blocks))
	metrics.DBStatsCrawledNodes.Set(float64(crawledNodes))
	metrics.DBStatsDiscNodes.Set(float64(discoveredNodes))
	metrics.DBStatsNodesToCrawl.Set(float64(toCrawl))
	metrics.DBStatsDiscNodesToCrawl.Set(float64(discToCrawl))
}

func (db *DB) lastFoundStats(ctx context.Context) {
	var err error

	defer metrics.ObserveDBQuery("last_found_stats", time.Now(), err)

	rows, err := db.pg.Query(
		ctx,
		`
			WITH last_found AS (
				SELECT
					time_bucket_gapfill(
						INTERVAL '3 hour',
						last_found,
						now() - INTERVAL '72 hours',
						now()
					) bucket,
					coalesce(COUNT(*), 0) count
				FROM disc.nodes
				WHERE last_found > now() - INTERVAL '72 hours'
				GROUP BY bucket
			)
			SELECT
				time_bucket(
					INTERVAL '3 hours',
					now()
				) - time_bucket(
					INTERVAL '3 hours',
					bucket
				) relative_bucket,
				count
			FROM last_found
			ORDER BY bucket DESC
		`,
	)
	if err != nil {
		log.Error("get last found stats query failed", "err", err)

		return
	}

	var bucket time.Duration
	var count int64

	_, err = pgx.ForEachRow(rows, []any{&bucket, &count}, func() error {
		metrics.LastFound.
			WithLabelValues(strconv.FormatInt(int64(bucket.Seconds()), 10)).
			Set(float64(count))

		return nil
	})
	if err != nil {
		log.Error("get last found stats rows failed", "err", err)
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

func (db *DB) WithTx(ctx context.Context, fn func(context.Context, pgx.Tx) error) error {
	tx, err := db.pg.Begin(ctx)
	if err != nil {
		return fmt.Errorf("start tx: %w", err)
	}
	defer tx.Rollback(ctx)

	err = fn(ctx, tx)
	if err != nil {
		return fmt.Errorf("fn: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
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
			log.Error("ephemery daemon failed", "err", err)
		}
	}
}
