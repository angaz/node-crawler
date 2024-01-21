package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/node-crawler/pkg/metrics"
)

// Meant to be run as a goroutine.
//
// Copies the stats into the stats table every `frequency` duration.
func (db *DB) CopyPortalStatsDaemon(ctx context.Context, frequency time.Duration) {
	for {
		nextRun := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(nextRun))

		err := db.CopyPortalStats(ctx)
		if err != nil {
			slog.Error("Copy portal stats failed", "err", err)
		}
	}
}

func (db *DB) CopyPortalStats(ctx context.Context) error {
	var err error

	defer metrics.ObserveDBQuery("copy_portal_stats", time.Now(), err)

	_, err = db.pg.Exec(
		ctx,
		`
			INSERT INTO portal.stats

			SELECT
				now() timestamp,
				client_name_id,
				client_version_id,
				country_geoname_id,
				dial_success,
				COUNT(*) total
			FROM portal.nodes_view
			WHERE
				last_found > (now() - INTERVAL '24 hours')
			GROUP BY
				client_name_id,
				client_version_id,
				country_geoname_id,
				dial_success
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}
