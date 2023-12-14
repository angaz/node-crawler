package database

import (
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/oschwald/geoip2-golang"
	"modernc.org/sqlite"
)

func init() {
	sqlite.RegisterDeterministicScalarFunction("best_record", 2, bestRecord)
}

type DB struct {
	db      *sql.DB
	geoipDB *geoip2.Reader

	nextCrawlSucces int64
	nextCrawlFail   int64
	nextCrawlNotEth int64

	wLock sync.Mutex
}

func NewAPIDB(db *sql.DB) *DB {
	return NewDB(db, nil, 0, 0, 0)
}

func NewDB(
	db *sql.DB,
	geoipDB *geoip2.Reader,
	nextCrawlSucces time.Duration,
	nextCrawlFail time.Duration,
	nextCrawlNotEth time.Duration,
) *DB {
	return &DB{
		db:      db,
		geoipDB: geoipDB,

		nextCrawlSucces: int64(nextCrawlSucces.Seconds()),
		nextCrawlFail:   int64(nextCrawlFail.Seconds()),
		nextCrawlNotEth: int64(nextCrawlNotEth.Seconds()),

		wLock: sync.Mutex{},
	}
}

func (db *DB) Close() error {
	return db.db.Close()
}

type tableStats struct {
	totalDiscoveredNodes   int64
	totalCrawledNodes      int64
	totalBlocks            int64
	totalToCrawl           int64
	databaseSizeBytes      int64
	databaseStatsSizeBytes int64
}

func (db *DB) getTableStats() (*tableStats, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("table_stats", start, err)

	rows, err := db.QueryRetryBusy(`
		SELECT
			(SELECT COUNT(*) FROM discovered_nodes),
			(
				SELECT COUNT(*) FROM discovered_nodes
				WHERE
					next_crawl < unixepoch()
					AND node_type IN (0, 1)
			),
			(SELECT COUNT(*) FROM crawled_nodes),
			(SELECT COUNT(*) FROM blocks),
			(
				SELECT page_count * page_size
				FROM pragma_page_count('main'), pragma_page_size('main')
			),
			(
				SELECT page_count * page_size
				FROM pragma_page_count('stats'), pragma_page_size('stats')
			)
	`)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		//nolint:exhaustruct
		stats := tableStats{}

		err = rows.Scan(
			&stats.totalDiscoveredNodes,
			&stats.totalToCrawl,
			&stats.totalCrawledNodes,
			&stats.totalBlocks,
			&stats.databaseSizeBytes,
			&stats.databaseStatsSizeBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		return &stats, nil
	}

	return nil, sql.ErrNoRows
}

// Meant to be run as a goroutine
//
// Periodically collects the table stat metrics
func (db *DB) TableStatsMetricsDaemon(frequency time.Duration) {
	for {
		next := time.Now().Add(frequency)

		stats, err := db.getTableStats()
		if err != nil {
			log.Error("get table stats failed", "err", err)

			continue
		}

		metrics.DBStatsBlocks.Set(float64(stats.totalBlocks))
		metrics.DBStatsCrawledNodes.Set(float64(stats.totalCrawledNodes))
		metrics.DBStatsDiscNodes.Set(float64(stats.totalDiscoveredNodes))
		metrics.DBStatsNodesToCrawl.Set(float64(stats.totalToCrawl))
		metrics.DBStatsSizeBytes.WithLabelValues("crawler").Set(float64(stats.databaseSizeBytes))
		metrics.DBStatsSizeBytes.WithLabelValues("stats").Set(float64(stats.databaseStatsSizeBytes))

		time.Sleep(time.Until(next))
	}
}

func retryBusy[T any](f func() (T, error)) (T, error) {
	retry := 0

	for {
		result, err := f()
		if err != nil && retry < 5 && strings.Contains(err.Error(), "database is locked (5) (SQLITE_BUSY)") {
			// retry 0: 2^0 * 50 + 100 = 150 ms
			// retry 1: 2^1 * 50 + 100 = 200 ms
			// retry 2: 2^2 * 50 + 100 = 300 ms
			// retry 3: 2^3 * 50 + 100 = 500 ms
			// retry 4: 2^4 * 50 + 100 = 900 ms
			time.Sleep(time.Duration((math.Pow(2, float64(retry))*50)+100) * time.Millisecond)

			retry += 1

			metrics.DatabaseRetries.WithLabelValues(strconv.Itoa(retry)).Inc()

			continue
		}

		return result, err
	}
}

func (d *DB) ExecRetryBusy(query string, args ...any) (sql.Result, error) {
	return retryBusy(func() (sql.Result, error) {
		return d.db.Exec(query, args...)
	})
}

func (db *DB) QueryRetryBusy(query string, args ...any) (*sql.Rows, error) {
	return retryBusy(func() (*sql.Rows, error) {
		return db.db.Query(query, args...)
	})
}

func bestRecord(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
	a, ok := args[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("first arg: not BLOB")
	}

	b, ok := args[1].([]byte)
	if !ok {
		return nil, fmt.Errorf("second arg: not BLOB")
	}

	recordA, err := common.LoadENR(a)
	if err != nil {
		return nil, fmt.Errorf("first arg: load enr: %w", err)
	}

	recordB, err := common.LoadENR(b)
	if err != nil {
		return nil, fmt.Errorf("second arg: load enr: %w", err)
	}

	return common.EncodeENR(common.BestRecord(recordA, recordB)), nil
}
