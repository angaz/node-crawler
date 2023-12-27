package database

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"
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

func (db *DB) Begin(ctx context.Context) (pgx.Tx, error) {
	return db.pg.Begin(ctx)
}

func (db *DB) getTableStats(ctx context.Context) (*tableStats, error) {
	var err error

	defer metrics.ObserveDBQuery("table_stats", time.Now(), err)

	rows, err := db.pg.Query(
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
				(SELECT COUNT(*) FROM execution.nodes),
				(SELECT COUNT(*) FROM execution.blocks)
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var stats tableStats

		err = rows.Scan(
			&stats.totalDiscoveredNodes,
			&stats.totalToCrawl,
			&stats.totalCrawledNodes,
			&stats.totalBlocks,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		return &stats, nil
	}

	return nil, sql.ErrNoRows
}

// Meant to be run as a goroutine
//
// Periodically collects the table stat metrics
func (db *DB) TableStatsMetricsDaemon(ctx context.Context, frequency time.Duration) {
	for ctx.Err() == nil {
		next := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(next))

		stats, err := db.getTableStats(ctx)
		if err != nil {
			log.Error("get table stats failed", "err", err)

			continue
		}

		metrics.DBStatsBlocks.Set(float64(stats.totalBlocks))
		metrics.DBStatsCrawledNodes.Set(float64(stats.totalCrawledNodes))
		metrics.DBStatsDiscNodes.Set(float64(stats.totalDiscoveredNodes))
		metrics.DBStatsNodesToCrawl.Set(float64(stats.totalToCrawl))
	}
}
