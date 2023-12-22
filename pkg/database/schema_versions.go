package database

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

type schemaVersion struct {
	Version   int
	Timestamp time.Time
}

type schemaVersionSlice []schemaVersion

func (s schemaVersionSlice) Exists(i int) bool {
	return slices.ContainsFunc(s, func(sv schemaVersion) bool {
		return sv.Version == i
	})
}

func schemaVersionsPG(ctx context.Context, tx pgx.Tx) (schemaVersionSlice, error) {
	rows, err := tx.Query(
		ctx,
		`
			SELECT
				version,
				timestamp
			FROM migrations.schema_versions
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	versions := []schemaVersion{}

	for rows.Next() {
		var sv schemaVersion

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
