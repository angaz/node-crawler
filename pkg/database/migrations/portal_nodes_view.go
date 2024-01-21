package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func PortalNodesView(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE OR REPLACE VIEW portal.nodes_view AS
				SELECT
					-- disc_nodes
					disc.node_id,
					first_found,
					last_found,
					next_disc_crawl.next_crawl next_disc_crawl,
					node_pubkey,
					node_record,
					ip_address,
					city_geoname_id,
					client_identifier_id,
					client_name_id,
					client_version_id,
					client_build_id,

					-- geoname.cities
					city_name,
					country_geoname_id,
					country_name,
					latitude,
					longitude,

					-- client.identifiers
					client_identifier,

					-- client.names
					client_name,

					-- client.versions
					client_version,

					-- client.builds
					client_build,

					FALSE dial_success
				FROM portal.disc_nodes disc
				LEFT JOIN portal.next_disc_crawl USING (node_id)
				LEFT JOIN client.identifiers USING (client_identifier_id)
				LEFT JOIN client.names USING (client_name_id)
				LEFT JOIN client.versions USING (client_version_id)
				LEFT JOIN client.builds USING (client_build_id)
				LEFT JOIN geoip.networks ON (
					disc.ip_address << geoip.networks.network
				)
				LEFT JOIN geoname.cities USING (city_geoname_id)
				LEFT JOIN geoname.countries USING (country_geoname_id)
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}
