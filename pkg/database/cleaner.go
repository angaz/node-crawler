package database

import (
	"context"
	"time"

	"log/slog"

	"github.com/ethereum/node-crawler/pkg/metrics"
)

// Meant to be run as a goroutine.
//
// Cleans old data from the database.
func (db *DB) CleanerDaemon(ctx context.Context, frequency time.Duration) {
	for {
		next := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(next))

		db.clean(ctx)
	}
}

func (db *DB) clean(ctx context.Context) {
	db.blocksCleaner(ctx)
	db.historyCleaner(ctx)
}

func (db *DB) blocksCleaner(ctx context.Context) {
	var err error

	defer metrics.ObserveDBQuery("blocks_clean", time.Now(), err)

	_, err = db.pg.Exec(
		ctx,
		`
			DELETE FROM execution.blocks
			WHERE (block_hash, network_id) IN (
				SELECT
					blocks.block_hash,
					blocks.network_id
				FROM execution.blocks
				LEFT JOIN execution.nodes ON (
					blocks.block_hash = nodes.head_hash
					AND blocks.network_id = nodes.network_id
				)
				WHERE nodes IS NULL
			)
		`,
	)
	if err != nil {
		slog.Error("blocks cleaner failed", "err", err)
	}
}

// Delete history of the accepted connections older than 14 days, or when there
// is a greater than 15 accepted connection rows. Some nodes make many requests
// per second. They will fill up the database pretty quickly otherwise.
// There is a large volume of accepted connections, so this will fill up the
// database significantly.
func (db *DB) historyCleaner(ctx context.Context) {
	var err error

	defer metrics.ObserveDBQuery("history_clean", time.Now(), err)

	_, err = db.pg.Exec(ctx, `
		DELETE FROM crawler.history
		WHERE (tableoid, ctid) IN (
			SELECT
				tableoid,
				ctid
			FROM (
				SELECT
					tableoid,
					ctid,
					crawled_at,
					ROW_NUMBER() OVER (PARTITION BY node_id, direction ORDER BY crawled_at) row_number
				FROM crawler.history
				WHERE
					direction = 'accept'
			)
			WHERE
				row_number > 15
				OR crawled_at < (now() - INTERVAL '14 days')
		)
	`)
	if err != nil {
		slog.Error("history cleaner failed", "err", err)
	}
}
