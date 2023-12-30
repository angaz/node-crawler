package migrations

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func createStatsView(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	interval time.Duration,
	startOffset time.Duration,
	retention time.Duration,
) error {
	_, err := tx.Exec(
		ctx,
		fmt.Sprintf(
			`
				CREATE MATERIALIZED VIEW %[1]s
					WITH (timescaledb.continuous) AS
				SELECT
					time_bucket(
						make_interval(secs => %[2]d),
						timestamp
					) bucket,
					client_name_id,
					client_version_id,
					client_os,
					client_arch,
					network_id,
					fork_id,
					next_fork_id,
					country_geoname_id,
					synced,
					dial_success,
					last(total, timestamp) total
				FROM stats.execution_nodes nodes
				GROUP BY
					bucket,
					client_name_id,
					client_version_id,
					client_os,
					client_arch,
					network_id,
					fork_id,
					next_fork_id,
					country_geoname_id,
					synced,
					dial_success
				ORDER BY
					bucket ASC
				WITH NO DATA;

				SELECT add_continuous_aggregate_policy(
					'%[1]s',
					start_offset => make_interval(secs => %[3]d),
					end_offset => INTERVAL '30 minutes',
					schedule_interval => INTERVAL '30 minutes'
				);

				SELECT add_retention_policy(
					'%[1]s',
					make_interval(secs => %[4]d)
				);
			`,
			fmt.Sprintf("%s_%s", tableName, interval.String()),
			int(interval.Seconds()),
			int(startOffset.Seconds()),
			int(retention.Seconds()),
		),
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func Migrate002StatsViews(ctx context.Context, tx pgx.Tx) error {
	err := createStatsView(
		ctx,
		tx,
		"stats.execution_nodes",
		30*time.Minute,
		2*time.Hour,
		6*24*time.Hour,
	)
	if err != nil {
		return fmt.Errorf("create view 30 minute: %w", err)
	}

	err = createStatsView(
		ctx,
		tx,
		"stats.execution_nodes",
		24*time.Hour,
		3*24*time.Hour,
		32*24*time.Hour,
	)
	if err != nil {
		return fmt.Errorf("create view daily: %w", err)
	}

	return nil
}
