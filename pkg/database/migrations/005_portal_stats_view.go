package migrations

import (
	"context"
	"fmt"
	"time"

	"log/slog"

	"github.com/jackc/pgx/v5"
)

func createPortalStatsView(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	interval time.Duration,
	startOffset time.Duration,
	retention time.Duration,
	chunkInterval time.Duration,
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
					) timestamp,
					client_name_id,
					client_version_id,
					country_geoname_id,
					dial_success,
					last(total, timestamp) total
				FROM portal.stats
				GROUP BY
					1,
					client_name_id,
					client_version_id,
					country_geoname_id,
					dial_success
				ORDER BY
					1 ASC
				WITH NO DATA;

				SELECT add_continuous_aggregate_policy(
					'%[1]s',
					start_offset => make_interval(secs => %[3]d),
					end_offset => INTERVAL '30 minutes',
					schedule_interval => make_interval(secs => %[2]d)
				);

				SELECT add_retention_policy(
					'%[1]s',
					make_interval(secs => %[4]d)
				);

				SELECT set_chunk_time_interval(
					'%[1]s',
					make_interval(secs => %[4]d)
				);
			`,
			tableName,
			int(interval.Seconds()),
			int(startOffset.Seconds()),
			int(retention.Seconds()),
			int(chunkInterval.Seconds()),
		),
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	slog.Warn(
		"Don't forget to add initial data",
		"query", fmt.Sprintf(
			"CALL refresh_continuous_aggregate('%s', '%s', INTERVAL '30 minutes');",
			tableName,
			time.Now().Add(-retention).Format("2006-01-02"),
		),
	)

	return nil
}

func Migrate005PortalStatsViews(ctx context.Context, tx pgx.Tx) error {
	err := createPortalStatsView(
		ctx,
		tx,
		"portal.stats_3h",
		3*time.Hour,
		9*time.Hour,
		14*day,
		3*day,
	)
	if err != nil {
		return fmt.Errorf("create view 3 hourly: %w", err)
	}

	err = createPortalStatsView(
		ctx,
		tx,
		"portal.stats_24h",
		24*time.Hour,
		3*day,
		32*day,
		7*day,
	)
	if err != nil {
		return fmt.Errorf("create view daily: %w", err)
	}

	return nil
}
