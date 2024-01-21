package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

/*

DROP SCHEMA client CASCADE;
DROP SCHEMA crawler CASCADE;
DROP SCHEMA network CASCADE;
DROP SCHEMA stats CASCADE;
DROP SCHEMA geoname CASCADE;
DROP SCHEMA disc CASCADE;
DROP SCHEMA execution CASCADE;
DROP SCHEMA consensus CASCADE;
DROP SCHEMA migrations CASCADE;

*/

func Migrate000Schema(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE EXTENSION IF NOT EXISTS timescaledb;

			CREATE SCHEMA client;
			CREATE SCHEMA consensus;
			CREATE SCHEMA crawler;
			CREATE SCHEMA disc;
			CREATE SCHEMA execution;
			CREATE SCHEMA geoname;
			CREATE SCHEMA network;
			CREATE SCHEMA stats;

			CREATE TYPE crawler.direction AS ENUM (
				'dial',
				'accept'
			);

			CREATE TYPE crawler.error AS ENUM (
				'disconnect requested',
				'network error',
				'breach of protocol',
				'useless peer',
				'too many peers',
				'already connected',
				'incompatible p2p protocol version',
				'invalid node identity',
				'client quitting',
				'unexpected identity',
				'connected to self',
				'read timeout',
				'DISCONNECT_RESERVED_0c',
				'DISCONNECT_RESERVED_0d',
				'DISCONNECT_RESERVED_0e',
				'DISCONNECT_RESERVED_0f',
				'subprotocol error',
				'DISCONNECT_RESERVED_11',
				'DISCONNECT_RESERVED_12',
				'DISCONNECT_RESERVED_13',
				'DISCONNECT_RESERVED_14',
				'DISCONNECT_RESERVED_15',
				'DISCONNECT_RESERVED_16',
				'DISCONNECT_RESERVED_17',
				'DISCONNECT_RESERVED_18',
				'DISCONNECT_RESERVED_19',
				'DISCONNECT_RESERVED_1a',
				'DISCONNECT_RESERVED_1b',
				'DISCONNECT_RESERVED_1c',
				'DISCONNECT_RESERVED_1d',
				'DISCONNECT_RESERVED_1e',
				'DISCONNECT_RESERVED_1f',
				'DISCONNECT_REASONS',
	
				'EOF',
				'unknown',
				'connection refused',
				'connection reset by peer',
				'corrupt input',
				'i/o timeout',
				'invalid message',
				'invalid public key',
				'network is unreachable',
				'no route to host',
				'protocol not available',
				'rlp decode',
				'broken pipe'
			);

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

			CREATE TABLE geoname.countries (
				country_geoname_id	INTEGER PRIMARY KEY,
				country_name		TEXT	NOT NULL
			);

			CREATE TABLE geoname.cities (
				city_geoname_id		INTEGER NOT NULL,
				city_name			TEXT	NOT NULL,
				country_geoname_id	INTEGER	NOT NULL REFERENCES geoname.countries (country_geoname_id),
				latitude			REAL	NOT NULL,
				longitude			REAL	NOT NULL,

				PRIMARY KEY (city_geoname_id) INCLUDE (country_geoname_id)
			);

			CREATE TABLE client.names (
				client_name_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_name		TEXT	NOT NULL,

				CONSTRAINT client_name_unique
					UNIQUE (client_name) INCLUDE (client_name_id)
			);

			CREATE TABLE client.user_data (
				client_user_data_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_user_data	TEXT	NOT NULL,

				CONSTRAINT client_user_data_unique
					UNIQUE (client_user_data) INCLUDE (client_user_data_id)
			);

			CREATE TABLE client.versions (
				client_version_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_version		TEXT	NOT NULL,

				CONSTRAINT client_version_unique
					UNIQUE (client_version) INCLUDE (client_version_id)
			);

			CREATE TABLE client.builds (
				client_build_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_build	TEXT	NOT NULL,

				CONSTRAINT client_build_unique
					UNIQUE (client_build) INCLUDE (client_build_id)
			);

			CREATE TABLE client.languages (
				client_language_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_language		TEXT	NOT NULL,

				CONSTRAINT client_language_unique
					UNIQUE (client_language) INCLUDE (client_language_id)
			);

			CREATE TABLE client.identifiers (
				client_identifier_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				client_identifier		TEXT		NOT NULL,
				client_name_id			INTEGER		DEFAULT NULL REFERENCES client.names (client_name_id),
				client_user_data_id		INTEGER		DEFAULT NULL REFERENCES client.user_data (client_user_data_id),
				client_version_id		INTEGER		DEFAULT NULL REFERENCES client.versions (client_version_id),
				client_build_id			INTEGER		DEFAULT NULL REFERENCES client.builds (client_build_id),
				client_os				client.os	NOT NULL,
				client_arch				client.arch	NOT NULL,
				client_language_id		INTEGER		DEFAULT NULL REFERENCES client.languages (client_language_id),

				CONSTRAINT client_identifier_unique
					UNIQUE (client_identifier) INCLUDE (client_identifier_id)
			);

			CREATE INDEX client_identifier_name ON client.identifiers (client_name_id);
			CREATE INDEX client_identifier_user_data ON client.user_data (client_user_data_id);
			CREATE INDEX client_identifier_version ON client.versions (client_version_id);
			CREATE INDEX client_identifier_build ON client.builds (client_build_id);

			CREATE TABLE network.forks (
				network_id			BIGINT	NOT NULL,
				block_time			BIGINT	NOT NULL,
				fork_id				BIGINT	NOT NULL,
				previous_fork_id	BIGINT	DEFAULT NULL,
				fork_name			TEXT	NOT NULL,
				network_name		TEXT	NOT NULL,

				CONSTRAINT fork_id_network_id
					UNIQUE (network_id, fork_id)
			);

			CREATE TABLE network.ephemery_releases (
				timestamp		TIMESTAMPTZ	NOT NULL UNIQUE,
				network_id		BIGINT		NOT NULL,
				network_name	TEXT		NOT NULL
			);

			CREATE TABLE stats.execution_nodes (
				timestamp			TIMESTAMPTZ	NOT NULL,
				client_name_id		INTEGER		DEFAULT NULL REFERENCES client.names (client_name_id),
				client_user_data_id	INTEGER		DEFAULT NULL REFERENCES client.user_data (client_user_data_id),
				client_version_id	INTEGER		DEFAULT NULL REFERENCES client.versions (client_version_id),
				client_os			client.os	NOT NULL,
				client_arch			client.arch	NOT NULL,
				network_id			BIGINT		NOT NULL,
				fork_id				BIGINT		NOT NULL,
				next_fork_id		BIGINT		NOT NULL,  -- 0 means no next fork
				country_geoname_id	INTEGER		DEFAULT NULL REFERENCES geoname.countries (country_geoname_id),
				synced				BOOLEAN		NOT NULL,
				dial_success		BOOLEAN 	NOT NULL,
				total				INTEGER 	NOT NULL
			);

			SELECT create_hypertable(
				'stats.execution_nodes',
				by_range('timestamp', INTERVAL '1 day')
			);

			CREATE TABLE disc.nodes (
				node_id			BYTEA				NOT NULL,
				node_type		client.node_type	NOT NULL,
				first_found		TIMESTAMPTZ			NOT NULL,
				node_pubkey		BYTEA				NOT NULL,
				node_record		BYTEA				NOT NULL,
				ip_address		INET				NOT NULL,
				city_geoname_id	INTEGER				NOT NULL REFERENCES geoname.cities (city_geoname_id),

				PRIMARY KEY (node_id) INCLUDE (node_record)
			) PARTITION BY RANGE (node_id);

			CREATE INDEX disc_nodes_ip_address ON
				disc.nodes (ip_address);

			CREATE TABLE crawler.next_disc_crawl (
				node_id		BYTEA		PRIMARY KEY REFERENCES disc.nodes (node_id),
				last_found	TIMESTAMPTZ	NOT NULL,
				next_crawl	TIMESTAMPTZ	NOT NULL
			);

			CREATE INDEX next_disc_crawl_next_crawl_node_id
				ON crawler.next_disc_crawl (next_crawl, node_id);
			CREATE INDEX next_disc_crawl_last_found
				ON crawler.next_disc_crawl (last_found);

			CREATE TABLE crawler.next_node_crawl (
				node_id		BYTEA				PRIMARY KEY REFERENCES disc.nodes (node_id),
				updated_at	TIMESTAMPTZ			DEFAULT NULL,
				next_crawl	TIMESTAMPTZ			NOT NULL,
				node_type	client.node_type	NOT NULL
			);

			CREATE INDEX next_node_crawl_execution_nodes_to_crawl
				ON crawler.next_node_crawl (next_crawl, node_id)
				WHERE node_type IN ('Unknown', 'Execution');

			CREATE TABLE execution.capabilities (
				capabilities_id	INTEGER	PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				capabilities	TEXT	NOT NULL,

				CONSTRAINT execution_capabilities_unique
					UNIQUE (capabilities) INCLUDE (capabilities_id)
			);

			CREATE TABLE execution.nodes (
				node_id					BYTEA	PRIMARY KEY REFERENCES disc.nodes (node_id),
				client_identifier_id	INTEGER	NOT NULL REFERENCES client.identifiers (client_identifier_id),
				rlpx_version			INTEGER	NOT NULL,
				capabilities_id			INTEGER	NOT NULL REFERENCES execution.capabilities (capabilities_id),
				network_id				BIGINT	NOT NULL,
				fork_id					BIGINT	NOT NULL,
				next_fork_id			BIGINT	NOT NULL,  -- 0 means no next fork
				head_hash				BYTEA	NOT NULL
			) PARTITION BY RANGE (node_id);

			CREATE TABLE execution.blocks (
				block_hash		BYTEA		NOT NULL,
				network_id		BIGINT		NOT NULL,
				block_number	BIGINT		NOT NULL,
				timestamp		TIMESTAMPTZ	NOT NULL,

				PRIMARY KEY (block_hash, network_id) INCLUDE (timestamp)
			);

			CREATE TABLE crawler.history (
				node_id		BYTEA				NOT NULL REFERENCES disc.nodes (node_id),
				crawled_at	TIMESTAMPTZ			NOT NULL,
				direction	crawler.direction	NOT NULL,
				error		crawler.error		DEFAULT NULL,

				PRIMARY KEY (node_id, crawled_at) INCLUDE (direction, error)
			);

			SELECT create_hypertable(
				'crawler.history',
				by_range('crawled_at', INTERVAL '1 day')
			);

			CREATE INDEX crawler_history_crawled_at
				ON crawler.history (crawled_at);

			CREATE INDEX crawler_history_dial_success
				ON crawler.history (node_id, crawled_at)
				WHERE
					direction = 'dial'
					AND (
						error IS NULL
						OR error < 'DISCONNECT_REASONS'
					);
		`,
	)
	if err != nil {
		return fmt.Errorf("create initial schema failed: %w", err)
	}

	err = createPartitions(ctx, tx, "disc.nodes")
	if err != nil {
		return fmt.Errorf("create partitions disc.nodes: %w", err)
	}
	err = createPartitions(ctx, tx, "execution.nodes")
	if err != nil {
		return fmt.Errorf("create partitions execution.nodes: %w", err)
	}

	return nil
}

func createPartitions(ctx context.Context, tx pgx.Tx, table string) error {
	end := 0x100
	skip := 0x10
	for i := 0; i < end; i += skip {
		from := fmt.Sprintf("'\\x%02X'", i)
		to := fmt.Sprintf("'\\x%02X'", i+skip)

		if i == 0 {
			from = "MINVALUE"
		}

		if i+skip >= end {
			to = "MAXVALUE"
		}

		_, err := tx.Exec(
			ctx,
			fmt.Sprintf(
				`
					CREATE TABLE %[1]s_%02[2]X
						PARTITION OF %[1]s
						FOR VALUES FROM (%[3]s) TO (%[4]s)
				`,
				table,
				i,
				from,
				to,
			),
		)
		if err != nil {
			return fmt.Errorf("create partition: %02X: %w", i, err)
		}
	}

	return nil
}
