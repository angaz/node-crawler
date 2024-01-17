package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func Migrate003GeoIP(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE SCHEMA geoip;

			CREATE TABLE geoip.current_build (
				build_timestamp	TIMESTAMPTZ	NOT NULL
			);

			CREATE TABLE geoip.networks (
				network			CIDR	NOT NULL,
				city_geoname_id	INTEGER NOT NULL REFERENCES geoname.cities (city_geoname_id)
			);

			CREATE INDEX network_city_geoname_id
				ON geoip.networks USING GIST (network inet_ops) INCLUDE (city_geoname_id);

			DROP VIEW execution.node_view;
			ALTER TABLE disc.nodes DROP COLUMN city_geoname_id;

			ALTER TABLE stats.execution_nodes ALTER country_geoname_id DROP NOT NULL;
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}
