package database

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"
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

func (db *DB) Migrate() error {
	return db.migrate(
		context.Background(),
		[]migrationFn{
			migrations.Migrate000Schema,
			func(ctx context.Context, tx pgx.Tx) error {
				if db.db == nil {
					return nil
				}

				return migrations.Migrate001SqliteToPG(ctx, tx, db.db, db.geoipDB)
			},
		},
		func(ctx context.Context, tx pgx.Tx) error {
			return migrations.InsertNetworks(ctx, tx, db.githubToken)
		},
		migrations.ClientUpsertStrings,
		migrations.ExecutionCapabilitiesUpsert,
	)
}

func (db *DB) migrate(
	ctx context.Context,
	migrations []migrationFn,
	staticObjects ...migrationFn,
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
		log.Info("running migration", "version", version)

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

		log.Info(
			"migration complete",
			"version", version,
			"duration", time.Since(start),
		)
	}

	for i, fn := range staticObjects {
		err = fn(ctx, tx)
		if err != nil {
			return fmt.Errorf("static object: %d failed: %w", i, err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit transaction failed: %w", err)
	}

	return nil
}
