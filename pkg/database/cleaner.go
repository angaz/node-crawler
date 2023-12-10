package database

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/metrics"
)

// Meant to be run as a goroutine.
//
// Cleans old data from the database.
func (db *DB) CleanerDaemon(frequency time.Duration) {
	for {
		nextClean := time.Now().Add(frequency)

		db.clean()

		time.Sleep(time.Until(nextClean))
	}
}

func (db *DB) clean() {
	ctx := context.Background()
	// ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	// defer cancel()

	db.blocksCleaner(ctx)
	db.historyCleaner(ctx)
}

func (db *DB) blocksCleaner(ctx context.Context) {
	db.wLock.Lock()
	defer db.wLock.Unlock()

	var err error

	defer metrics.ObserveDBQuery("blocks_clean", time.Now(), err)

	_, err = db.db.ExecContext(ctx, `
		DELETE FROM blocks
		WHERE (block_hash, network_id) IN (
			SELECT block_hash, blocks.network_id
			FROM blocks
			LEFT JOIN crawled_nodes crawled
				ON (blocks.block_hash = crawled.head_hash)
			WHERE crawled.node_id IS NULL
		)
	`)
	if err != nil {
		log.Error("blocks cleaner failed", "err", err)
	}
}

// Delete history of the accepted connections older than 14 days.
// There is a large volume of accepted connections, so this will fill up the
// database significantly.
func (db *DB) historyCleaner(ctx context.Context) {
	db.wLock.Lock()
	defer db.wLock.Unlock()

	var err error

	defer metrics.ObserveDBQuery("history_clean", time.Now(), err)

	_, err = db.db.ExecContext(ctx, `
		DELETE FROM crawl_history
		WHERE
			crawled_at < unixepoch('now', '-14 days')
			AND direction = 'accept'
	`)
	if err != nil {
		log.Error("history cleaner failed", "err", err)
	}
}
