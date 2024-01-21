package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func Migrate004Portal(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE SCHEMA portal;

			CREATE TABLE portal.disc_nodes (
				node_id					BYTEA		NOT NULL,
				first_found				TIMESTAMPTZ	NOT NULL,
				node_pubkey				BYTEA		NOT NULL,
				node_record				BYTEA		NOT NULL,
				ip_address				INET		NOT NULL,
				client_identifier_id	INTEGER		DEFAULT NULL,

				PRIMARY KEY (node_id) INCLUDE (node_record)
			) PARTITION BY RANGE (node_id);

			CREATE INDEX disc_nodes_ip_address ON
				portal.disc_nodes (ip_address);

			CREATE TABLE portal.next_disc_crawl (
				node_id		BYTEA		PRIMARY KEY REFERENCES portal.disc_nodes (node_id),
				last_found	TIMESTAMPTZ	NOT NULL,
				next_crawl	TIMESTAMPTZ	NOT NULL
			);

			CREATE TABLE portal.stats (
				timestamp			TIMESTAMPTZ	NOT NULL,
				client_name_id		INTEGER		DEFAULT NULL REFERENCES client.names (client_name_id),
				client_version_id	INTEGER		DEFAULT NULL REFERENCES client.versions (client_version_id),
				country_geoname_id	INTEGER		DEFAULT NULL REFERENCES geoname.countries (country_geoname_id),
				dial_success		BOOLEAN 	NOT NULL,
				total				INTEGER 	NOT NULL
			);

			SELECT create_hypertable(
				'portal.stats',
				by_range('timestamp', INTERVAL '1 day')
			);
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	err = createPartitions(ctx, tx, "portal.disc_nodes")
	if err != nil {
		return fmt.Errorf("create partitions portal.disc_nodes: %w", err)
	}

	return nil
}
