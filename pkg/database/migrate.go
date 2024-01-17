package database

import (
	"context"
	"fmt"
	"time"

	"log/slog"

	"github.com/ethereum/node-crawler/pkg/database/migrations"
	"github.com/jackc/pgx/v5"
)

type migrationFn func(context.Context, pgx.Tx) error

func migrateCommand(sql string) migrationFn {
	return func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("exec: %w", err)
		}

		return nil
	}
}

func (db *DB) Migrate(geoipdb string) error {
	return db.migrate(
		context.Background(),
		[]migrationFn{
			migrations.Migrate000Schema,
			func(ctx context.Context, tx pgx.Tx) error {
				if db.db == nil {
					return nil
				}

				// TODO: Fix the geoipdb usage here.
				return migrations.Migrate001SqliteToPG(ctx, tx, db.db, nil)
			},
			migrations.Migrate002StatsViews,
			migrations.Migrate003GeoIP,
		},
		map[string]migrationFn{
			"insert networks": func(ctx context.Context, tx pgx.Tx) error {
				return migrations.InsertNetworks(ctx, tx, db.githubToken)
			},
			"function client.upsert":                 migrations.ClientUpsertStrings,
			"function execution.capabilities_upsert": migrations.ExecutionCapabilitiesUpsert,
			"geoip": func(ctx context.Context, tx pgx.Tx) error {
				return migrations.UpdateGeoIPData(ctx, tx, geoipdb)
			},
			"execution.node_view": migrations.ExecutionNodeView,
		},
	)
}

func (db *DB) migrate(
	ctx context.Context,
	migrations []migrationFn,
	staticObjects map[string]migrationFn,
) error {
	tx, err := db.pg.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(
		ctx,
		`
			SELECT pg_advisory_lock(0x9c538d3d);  -- crc32.ChecksumIEEE('node-crawler')
		`,
	)
	if err != nil {
		return fmt.Errorf("take lock: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
			CREATE SCHEMA IF NOT EXISTS migrations;

			CREATE TABLE IF NOT EXISTS migrations.schema_versions (
				version		INTEGER		PRIMARY KEY,
				timestamp	TIMESTAMPTZ	NOT NULL
			);
		`,
	)
	if err != nil {
		return fmt.Errorf("creating schema version table failed: %w", err)
	}

	schemaVersions, err := schemaVersionsPG(ctx, tx)
	if err != nil {
		return fmt.Errorf("selecting schema version failed: %w", err)
	}

	for version, migration := range migrations {
		if schemaVersions.Exists(version) {
			continue
		}

		start := time.Now()
		slog.Info("migration start", "version", version)

		err := migration(ctx, tx)
		if err != nil {
			return fmt.Errorf("migration (%d) failed: %w", version, err)
		}

		_, err = tx.Exec(
			ctx,
			`
				INSERT INTO migrations.schema_versions (
					version, timestamp
				) VALUES (
					$1, now()
				)
			`,
			version,
		)
		if err != nil {
			return fmt.Errorf("insert migration failed: %w", err)
		}

		slog.Info(
			"migration complete",
			"version", version,
			"duration", time.Since(start),
		)
	}

	for name, fn := range staticObjects {
		start := time.Now()
		slog.Info("migration start", "name", name)

		err = fn(ctx, tx)
		if err != nil {
			return fmt.Errorf("static object: %s failed: %w", name, err)
		}

		slog.Info("migration complete", "name", name, "duration", time.Since(start))
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit transaction failed: %w", err)
	}

	return nil
}
