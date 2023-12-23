package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func Migrate000Schema(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE EXTENSION IF NOT EXISTS timescaledb;

			CREATE SCHEMA client;
			CREATE SCHEMA network;
			CREATE SCHEMA stats;
			CREATE SCHEMA geoname;

			CREATE TYPE client.node_type AS ENUM (
				'Unknown',
				'Execution',
				'Consensus'
			);

			CREATE TYPE client.os AS ENUM (
				'Unknown',
				'Android',
				'FreeBSD',
				'Linux',
				'MacOS',
				'Windows'
			);

			CREATE TYPE client.arch AS ENUM (
				'Unknown',
				'amd64',
				'arm64',
				'i386',
				'IBM System/390'
			);

			CREATE TABLE geoname.cities (
				city_geoname_id	INTEGER PRIMARY KEY,
				city_name		TEXT	NOT NULL,
				latitude		REAL	NOT NULL,
				longitude		REAL	NOT NULL
			);

			CREATE TABLE geoname.countries (
				country_geoname_id	INTEGER PRIMARY KEY,
				country_name		TEXT	NOT NULL
			);

			CREATE TABLE client.client_names (
				client_name_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_name		TEXT	NOT NULL,

				CONSTRAINT client_name_unique
					UNIQUE (client_name) INCLUDE (client_name_id)
			);

			CREATE TABLE client.client_user_data (
				client_user_data_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_user_data	TEXT	NOT NULL,

				CONSTRAINT client_user_data_unique
					UNIQUE (client_user_data) INCLUDE (client_user_data_id)
			);

			CREATE TABLE client.client_versions (
				client_version_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_version		TEXT	NOT NULL,

				CONSTRAINT client_version_unique
					UNIQUE (client_version) INCLUDE (client_version_id)
			);

			CREATE TABLE network.forks (
				network_id			BIGINT	NOT NULL,
				block_time			BIGINT	NOT NULL,
				fork_id				BIGINT	NOT NULL,
				previous_fork_id	BIGINT	DEFAULT NULL,
				fork_name			TEXT	NOT NULL,
				network_name		TEXT	NOT NULL,

				CONSTRAINT fork_id_network_id UNIQUE (network_id, fork_id)
			);

			CREATE TABLE network.ephemery_releases (
				timestamp	TIMESTAMPTZ	NOT NULL UNIQUE,
				name		TEXT		NOT NULL
			);

			CREATE TABLE stats.execution_nodes (
				timestamp			TIMESTAMPTZ			NOT NULL,
				client_name_id		INTEGER				DEFAULT NULL REFERENCES client.client_names(client_name_id),
				client_user_data_id	INTEGER				DEFAULT NULL REFERENCES client.client_user_data(client_user_data_id),
				client_version_id	INTEGER				DEFAULT NULL REFERENCES client.client_versions(client_version_id),
				client_os			client.os			NOT NULL,
				client_arch			client.arch			NOT NULL,
				network_id			BIGINT				NOT NULL,
				fork_id				BIGINT				NOT NULL,
				next_fork_id		BIGINT				DEFAULT NULL,
				country_geoname_id	INTEGER				NOT NULL REFERENCES geoname.countries(country_geoname_id),
				synced				BOOLEAN				NOT NULL,
				dial_success		BOOLEAN 			NOT NULL,
				total				INTEGER 			NOT NULL
			);

			SELECT create_hypertable('stats.execution_nodes', by_range('timestamp'));
		`,
	)
	if err != nil {
		return fmt.Errorf("create initial schema failed: %w", err)
	}

	return nil
}
