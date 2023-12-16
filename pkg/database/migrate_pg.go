package database

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/jackc/pgx/v5"
)

type migrationFnPG func(context.Context, pgx.Tx) error

func (db *DB) migratePG(
	ctx context.Context,
	migrations []migrationFnPG,
	staticObjects migrationFnPG,
) error {
	tx, err := db.pg.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback(ctx)

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

	err = staticObjects(ctx, tx)
	if err != nil {
		return fmt.Errorf("create indexes failed: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit transaction failed: %w", err)
	}

	return nil
}

type schemaVersionPG struct {
	Version   int
	Timestamp time.Time
}

type schemaVersionSlicePG []schemaVersionPG

func (s schemaVersionSlicePG) Exists(i int) bool {
	return slices.ContainsFunc(s, func(sv schemaVersionPG) bool {
		return sv.Version == i
	})
}

func schemaVersionsPG(ctx context.Context, tx pgx.Tx) (schemaVersionSlicePG, error) {
	rows, err := tx.Query(
		ctx,
		"SELECT version, timestamp FROM migrations.schema_versions",
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	versions := []schemaVersionPG{}

	for rows.Next() {
		var sv schemaVersionPG

		err := rows.Scan(&sv.Version, &sv.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}

		versions = append(versions, sv)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows failed: %w", err)
	}

	return versions, nil
}
