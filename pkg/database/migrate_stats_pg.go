package database

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

func (db *DB) MigrateStatsPG() error {
	return db.migratePG(
		context.Background(),
		[]migrationFnPG{
			migrateStats000SchemaPG,
			db.migrateStats001CopyDataPG,
		},
		insertForkIDs,
	)
}

func insertForkIDs(ctx context.Context, tx pgx.Tx) error {

	return nil
}

func migrateStats000SchemaPG(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE EXTENSION IF NOT EXISTS timescaledb;

			CREATE SCHEMA IF NOT EXISTS client;
			CREATE SCHEMA IF NOT EXISTS network;
			CREATE SCHEMA IF NOT EXISTS stats;

			CREATE TYPE client.node_type AS ENUM (
				'Unknown',
				'Execution',
				'Consensus'
			);

			CREATE TYPE IF NOT EXISTS client.os AS ENUM (
				'Unknown',
				'Android',
				'FreeBSD'
				'Linux',
				'MacOS',
				'Windows'
			);

			CREATE TYPE IF NOT EXISTS client.arch AS ENUM (
				'Unknown',
				'amd64',
				'arm64',
				'i386',
				'IBM System/390'
			);

			CREATE TABLE IF NOT EXISTS client.client_names (
				client_name_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_name		TEXT	NOT NULL,

				CONSTRAINT client_name_unique
					UNIQUE (client_name) INCLUDE (client_name_id)
			);

			CREATE TABLE IF NOT EXISTS client.client_user_data (
				client_user_data_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_user_data	TEXT	NOT NULL,

				CONSTRAINT client_user_data_unique
					UNIQUE (client_user_data) INCLUDE (client_user_data_id)
			);

			CREATE TABLE IF NOT EXISTS client.client_versions (
				client_version_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_version		TEXT	NOT NULL,

				CONSTRAINT client_version_unique
					UNIQUE (client_version) INCLUDE (client_version_id)
			);

			CREATE TABLE IF NOT EXISTS network.forks (
				network_id			BIGINT	NOT NULL,
				block_time			BIGINT	NOT NULL,
				fork_id				BIGINT	NOT NULL,
				previous_fork_id	BIGINT	DEFAULT NULL,
				name				TEXT	NOT NULL,

				CONSTRAINT fork_id_network_id UNIQUE (network_id, fork_id)
			);

			CREATE TABLE IF NOT EXISTS stats.crawled_nodes (
				timestamp			TIMESTAMPTZ			NOT NULL,
				node_type			client.node_type	NOT NULL,
				client_name_id		INTEGER				DEFAULT NULL REFERENCES client.client_names(client_name_id),
				client_user_data_id	INTEGER				DEFAULT NULL REFERENCES client.client_user_data(client_user_data_id),
				client_version_id	INTEGER				DEFAULT NULL REFERENCES client.client_versions(client_version_id),
				client_os			client.os			NOT NULL,
				client_arch			client.arch			NOT NULL,
				network_id			BIGINT				NOT NULL,
				fork_id				BIGINT				NOT NULL,
				next_fork_id		BIGINT				DEFAULT NULL,
				country				INTEGER				NOT NULL,
				synced				BOOLEAN				NOT NULL,
				dial_success		BOOLEAN 			NOT NULL,
				total				INTEGER 			NOT NULL,
			);

			SELECT create_hypertable('stats.crawled_nodes', by_range('timestamp'));
		`,
	)
	if err != nil {
		return fmt.Errorf("create initial schema failed: %w", err)
	}

	return nil
}

func (db *DB) migrateStats001CopyDataPG(ctx context.Context, tx pgx.Tx) error {
	clientNames := map[string]int64{}
	clientVersions := map[string]int64{}
	clientUserDatas := map[string]int64{}

	rows, err := db.db.QueryContext(
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

	rows, err = db.db.QueryContext(
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
	rows, err = db.db.QueryContext(
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

	rows, err = db.db.QueryContext(
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

		var os OS
		var arch Arch

		if clientOS != nil {
			idx := slices.Index(osStrings, *clientOS)
			os = OS(idx)
		}

		if clientArch != nil {
			idx := slices.Index(archStrings, *clientArch)
			arch = Arch(idx)
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
			coutnry_id,
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
