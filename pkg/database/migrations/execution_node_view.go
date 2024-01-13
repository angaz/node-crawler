package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func ExecutionNodeView(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE OR REPLACE VIEW execution.node_view AS
				SELECT
					-- disc.nodes
					disc.node_id,
					disc.node_type,
					first_found,
					last_found,
					next_disc_crawl.next_crawl next_disc_crawl,
					next_node_crawl.next_crawl next_node_crawl,
					node_pubkey,
					node_record,
					ip_address,
					city_geoname_id,

					-- execution.nodes
					updated_at,
					client_identifier_id,
					rlpx_version,
					capabilities_id,
					crawled.network_id,
					crawled.fork_id,
					next_fork_id,
					head_hash,
					client_name_id,
					client_user_data_id,
					client_version_id,
					client_build_id,
					client_os,
					client_arch,
					client_language_id,

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

					-- client.user_data
					client_user_data,

					-- client.versions
					client_version,

					-- client.builds
					client_build,

					-- client.languages
					client_language,

					-- execution.capabilities
					capabilities,

					-- execution.blocks
					block_hash,
					block_number,
					timestamp,

					-- network.forks
					forks.block_time,
					forks.previous_fork_id,
					forks.fork_name,
					forks.network_name,

					-- next_fork
					next_fork.block_time next_fork_block_time,
					next_fork.fork_name next_fork_name,
					next_fork.network_name next_fork_network_name,

					CASE
						WHEN blocks.timestamp IS NULL
							THEN FALSE
						WHEN next_node_crawl.updated_at > blocks.timestamp
							THEN (next_node_crawl.updated_at - blocks.timestamp) < INTERVAL '1 minute'
						ELSE FALSE
					END synced,
					EXISTS (
						SELECT 1
						FROM crawler.history
						WHERE
							history.node_id = crawled.node_id
							AND history.direction = 'dial'
							AND history.crawled_at > (now() - INTERVAL '7 days')
							AND (
								history.error IS NULL
								OR history.error < 'DISCONNECT_REASONS'
							)
					) dial_success
				FROM disc.nodes disc
				LEFT JOIN execution.nodes crawled USING (node_id)
				LEFT JOIN crawler.next_disc_crawl USING (node_id)
				LEFT JOIN crawler.next_node_crawl USING (node_id)
				LEFT JOIN client.identifiers USING (client_identifier_id)
				LEFT JOIN client.names USING (client_name_id)
				LEFT JOIN client.user_data USING (client_user_data_id)
				LEFT JOIN client.versions USING (client_version_id)
				LEFT JOIN client.builds USING (client_build_id)
				LEFT JOIN client.languages USING (client_language_id)
				LEFT JOIN execution.capabilities USING (capabilities_id)
				LEFT JOIN geoname.cities USING (city_geoname_id)
				LEFT JOIN geoname.countries USING (country_geoname_id)
				LEFT JOIN execution.blocks ON (
					crawled.head_hash = blocks.block_hash
					AND crawled.network_id = blocks.network_id
				)
				LEFT JOIN network.forks ON (
					crawled.network_id = forks.network_id
					AND crawled.fork_id = forks.fork_id
				)
				LEFT JOIN network.forks next_fork ON (
					crawled.network_id = next_fork.network_id
					AND crawled.next_fork_id = next_fork.block_time
				)
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}
