package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/jackc/pgx/v5"
)

func Migrate001SqliteToPG(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) error {
	clientNames := map[string]int64{}
	clientVersions := map[string]int64{}
	clientUserDatas := map[string]int64{}

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT DISTINCT client_name
			FROM stats.crawled_nodes
			WHERE client_name IS NOT NULL
		`,
	)
	if err != nil {
		return fmt.Errorf("client_names query: %w", err)
	}
	defer rows.Close()

	stmt, err := tx.Prepare(
		ctx,
		"insert_client_names",
		`
			INSERT INTO stats.client_names (
				client_name
			) VALUES (
				$1
			)
			ON CONFLICT (client_name) DO NOTHING
			RETURNING client_name_id
		`,
	)
	if err != nil {
		return fmt.Errorf("client_names prepare: %w", err)
	}

	for rows.Next() {
		var name string
		var id int64

		err = rows.Scan(&name)
		if err != nil {
			return fmt.Errorf("client_names scan: %w", err)
		}

		row := tx.QueryRow(
			ctx,
			stmt.Name,
			name,
		)

		err = row.Scan(&id)
		if err != nil {
			return fmt.Errorf("client_names insert: %w", err)
		}

		clientNames[name] = id
	}

	rows.Close()

	rows, err = sqlite.QueryContext(
		ctx,
		`
			SELECT DISTINCT client_version
			FROM stats.crawled_nodes
			WHERE client_version IS NOT NULL
		`,
	)
	if err != nil {
		return fmt.Errorf("client_version query: %w", err)
	}
	defer rows.Close()

	stmt, err = tx.Prepare(
		ctx,
		"insert_client_version",
		`
			INSERT INTO stats.client_versions (
				client_version
			) VALUES (
				$1
			)
			ON CONFLICT (client_version) DO NOTHING
			RETURNING client_version_id
		`,
	)
	if err != nil {
		return fmt.Errorf("client_version prepare: %w", err)
	}

	for rows.Next() {
		var version string
		var id int64

		err = rows.Scan(&version)
		if err != nil {
			return fmt.Errorf("client_version scan: %w", err)
		}

		row := tx.QueryRow(
			ctx,
			stmt.Name,
			version,
		)

		err = row.Scan(&id)
		if err != nil {
			return fmt.Errorf("client_version insert: %w", err)
		}

		clientVersions[version] = id
	}

	rows.Close()
	rows, err = sqlite.QueryContext(
		ctx,
		`
			SELECT DISTINCT client_user_data
			FROM stats.crawled_nodes
			WHERE client_user_data IS NOT NULL
		`,
	)
	if err != nil {
		return fmt.Errorf("client_user_data query: %w", err)
	}
	defer rows.Close()

	stmt, err = tx.Prepare(
		ctx,
		"insert_client_user_data",
		`
			INSERT INTO stats.client_user_data (
				client_user_data
			) VALUES (
				$1
			)
			ON CONFLICT (client_user_data) DO NOTHING
			RETURNING client_user_data_id
		`,
	)
	if err != nil {
		return fmt.Errorf("client_user_data prepare: %w", err)
	}

	for rows.Next() {
		var userData string
		var id int64

		err = rows.Scan(&userData)
		if err != nil {
			return fmt.Errorf("client_user_data scan: %w", err)
		}

		row := tx.QueryRow(
			ctx,
			stmt.Name,
			userData,
		)

		err = row.Scan(&id)
		if err != nil {
			return fmt.Errorf("client_user_data insert: %w", err)
		}

		clientUserDatas[userData] = id
	}

	rows.Close()

	rows, err = sqlite.QueryContext(
		ctx,
		`
			SELECT
				timestamp,
				client_name,
				client_user_data,
				client_version,
				client_os,
				client_arch,
				network_id,
				fork_id,
				next_fork_id,
				country,
				synced,
				dial_success,
				total
			FROM stats.crawled_nodes
		`,
	)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	copier := pgx.CopyFromFunc(func() ([]any, error) {
		if !rows.Next() {
			return nil, nil
		}

		var timestamp int64
		var clientName *string
		var clientUserData *string
		var clientVersion *string
		var clientOS *string
		var clientArch *string
		var networkID int64
		var forkID int64
		var nextForkID *uint64
		var country string
		var synced bool
		var dialSuccess bool
		var total int64

		err := rows.Scan(
			&timestamp,
			&clientName,
			&clientUserData,
			&clientVersion,
			&clientOS,
			&clientArch,
			&networkID,
			&forkID,
			&nextForkID,
			&country,
			&synced,
			&dialSuccess,
			&total,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		var os common.OS
		var arch common.Arch

		if clientOS != nil {
			os = common.OSIndex(*clientOS)
		}

		if clientArch != nil {
			arch = common.ArchIndex(*clientArch)
		}

		var cnID, cudID, cvID *int64

		if clientName != nil {
			id := clientNames[*clientName]
			cnID = &id
		}

		if clientUserData != nil {
			id := clientUserDatas[*clientUserData]
			cudID = &id
		}

		if clientVersion != nil {
			id := clientVersions[*clientVersion]
			cvID = &id
		}

		return []any{
			time.Unix(timestamp, 0),
			cnID,
			cudID,
			cvID,
			os,
			arch,
			networkID,
			forkID,
			nextForkID,
			// TODO
			// country_id,
			0,
			synced,
			dialSuccess,
			total,
		}, nil
	})

	_, err = tx.CopyFrom(ctx, []string{"stats", "crawled_nodes"}, []string{
		"timestamp",
		"client_name_id",
		"client_user_data_id",
		"client_version_id",
		"client_os",
		"client_arch",
		"network_id",
		"fork_id",
		"next_fork_id",
		"country",
		"synced",
		"dial_success",
		"total",
	}, copier)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}
