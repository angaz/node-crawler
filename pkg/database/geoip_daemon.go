package database

import (
	"context"
	"time"

	"github.com/ethereum/node-crawler/pkg/database/migrations"
	"github.com/jackc/pgx/v5"
	"golang.org/x/exp/slog"
)

// Meant to be run as a goroutine.
//
// Periodically updates the geoip data based on the given geoip database file.
func (db *DB) UpdateGeoIPDaemon(ctx context.Context, frequency time.Duration, geoipdb string) {
	for {
		next := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(next))

		err := db.WithTx(
			ctx,
			func(ctx context.Context, tx pgx.Tx) error {
				return migrations.UpdateGeoIPData(ctx, tx, geoipdb)
			},
		)
		if err != nil {
			slog.Error("update geoip data failed", "err", err)
		}
	}
}
