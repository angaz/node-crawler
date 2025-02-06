package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func Migrate006Consensus(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE SCHEMA consensus;

			CREATE TABLE consensus.nodes (
				node_id				BYTEA		PRIMARY KEY REFERENCES disc.nodes (node_id),
				fork_digest			BIGINT		NOT NULL,
				next_fork_version	INTEGER		NOT NULL,
				next_fork_epoch		BIGINT		NOT NULL,
			) PARTITION BY RANGE (node_id);

			CREATE TABLE consensus.stats (
				timestamp			TIMESTAMPTZ	NOT NULL,
				client_name_id		INTEGER		DEFAULT NULL REFERENCES client.names (client_name_id),
				client_version_id	INTEGER		DEFAULT NULL REFERENCES client.versions (client_version_id),
				country_geoname_id	INTEGER		DEFAULT NULL REFERENCES geoname.countries (country_geoname_id),
				attnets_count		INTEGER		NOT NULL,
				dial_success		BOOLEAN		NOT NULL,
				total				INTEGER		NOT NULL
			);

			SELECT create_hypertable(
				'consensus.stats',
				by_range('timestamp', INTERVAL '1 day')
			);
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	err = createPartitions(ctx, tx, "consensus.nodes")
	if err != nil {
		return fmt.Errorf("create partitions consensus.nodes: %w", err)
	}

	return nil
}
