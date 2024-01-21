package database

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"text/template"
	"time"

	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/jackc/pgx/v5"
)

// Meant to be run as a goroutine.
//
// Copies the stats into the stats table every `frequency` duration.
func (db *DB) CopyPortalStatsDaemon(ctx context.Context, frequency time.Duration) {
	for {
		nextRun := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(nextRun))

		err := db.CopyPortalStats(ctx)
		if err != nil {
			slog.Error("Copy portal stats failed", "err", err)
		}
	}
}

func (db *DB) CopyPortalStats(ctx context.Context) error {
	var err error

	defer metrics.ObserveDBQuery("copy_portal_stats", time.Now(), err)

	_, err = db.pg.Exec(
		ctx,
		`
			INSERT INTO portal.stats

			SELECT
				now() timestamp,
				client_name_id,
				client_version_id,
				country_geoname_id,
				dial_success,
				COUNT(*) total
			FROM portal.nodes_view
			WHERE
				last_found > (now() - INTERVAL '24 hours')
			GROUP BY
				client_name_id,
				client_version_id,
				country_geoname_id,
				dial_success
		`,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

// !!! `column` IS NOT SANITIZED !!! Do not use user-provided values.
func portalStatsGraph(
	ctx context.Context,
	tx pgx.Tx,
	column string,
) ([]StatsGraphSeries, error) {
	rows, err := tx.Query(
		ctx,
		fmt.Sprintf(
			`
				WITH instant AS (
					SELECT
						%[1]s key,
						SUM(total)::INTEGER total
					FROM timeseries
					LEFT JOIN client.names USING (client_name_id)
					LEFT JOIN client.versions USING (client_version_id)
					LEFT JOIN geoname.countries USING (country_geoname_id)
					WHERE
						timestamp = (SELECT MAX(timestamp) FROM timeseries WHERE total IS NOT NULL)
					GROUP BY
						key
					ORDER BY total DESC
				), grouped AS (
					SELECT
						timestamp,
						%[1]s key,
						SUM(total)::INTEGER total
					FROM timeseries
					LEFT JOIN client.names USING (client_name_id)
					LEFT JOIN client.versions USING (client_version_id)
					GROUP BY
						timestamp,
						key
				)

				SELECT
					grouped.key,
					array_agg(grouped.total ORDER BY grouped.timestamp) totals
				FROM grouped
				LEFT JOIN instant USING (key)
				GROUP BY key
				ORDER BY MAX(instant.total) ASC NULLS FIRST
			`,
			column,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	graph, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (StatsGraphSeries, error) {
		var series StatsGraphSeries

		err := rows.Scan(
			&series.key,
			&series.Totals,
		)

		return series, err
	})
	if err != nil {
		return nil, fmt.Errorf("collect: %w", err)
	}

	return graph, nil
}

var portalStatsTempl = template.Must(template.New("portal_stats_temp_table").Parse(`
	SELECT
		{{ if not .Instant }}
		time_bucket_gapfill(
			make_interval(hours => @interval),
			timestamp,
			@after::TIMESTAMPTZ,
			@before::TIMESTAMPTZ
		) timestamp,
		{{ end }}
		client_name_id,
		client_version_id,
		country_geoname_id,
		dial_success,
		last(total, timestamp) total
	INTO TEMPORARY TABLE {{ .TempTableName }}
	FROM {{ .FromTableName }} stats
	LEFT JOIN client.names USING (client_name_id)
	WHERE
		{{ if .Instant }}
		stats.timestamp = (SELECT MAX(timestamp) FROM {{ .FromTableName }})
		{{ else }}
		timestamp >= @after::TIMESTAMPTZ
		AND timestamp < @before::TIMESTAMPTZ
		{{ end }}
		AND (
			@client_name = ''
			OR names.client_name = @client_name
		)
	GROUP BY
		{{ if not .Instant }}1,  -- timestamp{{ end }}
		client_name_id,
		client_version_id,
		country_geoname_id,
		dial_success
	{{ if not .Instant }}
	ORDER BY
		1 ASC  -- timestamp
	{{ end }}
`))

func portalStatsTempTable(tempTableName string, fromTableName string, instant bool) string {
	builder := new(strings.Builder)

	err := portalStatsTempl.Execute(builder, struct {
		Instant       bool
		TempTableName string
		FromTableName string
	}{
		instant,
		tempTableName,
		fromTableName,
	})
	if err != nil {
		panic("execute template: " + err.Error())
	}

	return builder.String()
}

func (db *DB) GetPortalStats(
	ctx context.Context,
	after time.Time,
	before time.Time,
	clientName string,
	graphInterval time.Duration,
) (*StatsResult, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_portal_stats", start, err)

	tx, err := db.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	intervalHours := int(graphInterval.Hours())
	statsTempParams := pgx.NamedArgs{
		"interval":    intervalHours,
		"after":       after.Format(time.RFC3339),
		"before":      before.Format(time.RFC3339),
		"client_name": clientName,
	}

	_, err = tx.Exec(
		ctx,
		portalStatsTempTable(
			"timeseries",
			fmt.Sprintf("portal.stats_%dh", intervalHours),
			false,
		),
		statsTempParams,
	)
	if err != nil {
		return nil, fmt.Errorf("create timeseries temp table: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		portalStatsTempTable(
			"timeseries_instant",
			"portal.stats",
			true,
		),
		statsTempParams,
	)
	if err != nil {
		return nil, fmt.Errorf("create instant temp table: %w", err)
	}

	rows, err := tx.Query(ctx, `SELECT DISTINCT timestamp FROM timeseries ORDER BY timestamp`)
	if err != nil {
		return nil, fmt.Errorf("query buckets: %w", err)
	}
	defer rows.Close()

	timestamps, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (time.Time, error) {
		var ts time.Time

		err := row.Scan(&ts)

		return ts, err
	})
	if err != nil {
		return nil, fmt.Errorf("collect timestamps: %w", err)
	}

	var clientNameGraph []StatsGraphSeries
	var clientNameInstant []StatsSeriesInstant

	if clientName == "" {
		clientNameGraph, err = portalStatsGraph(ctx, tx, "client_name")
		if err != nil {
			return nil, fmt.Errorf("client_name graph: %w", err)
		}

		clientNameInstant, err = statsInstant(ctx, tx, "client_name")
		if err != nil {
			return nil, fmt.Errorf("client_name instant: %w", err)
		}
	} else {
		clientNameGraph, err = portalStatsGraph(ctx, tx, "client_version")
		if err != nil {
			return nil, fmt.Errorf("client_version graph: %w", err)
		}

		clientNameInstant, err = statsInstant(ctx, tx, "client_version")
		if err != nil {
			return nil, fmt.Errorf("client_version instant: %w", err)
		}
	}

	dialSuccessGraph, err := portalStatsGraph(ctx, tx, "CASE WHEN dial_success THEN 'Success' ELSE 'Fail' END")
	if err != nil {
		return nil, fmt.Errorf("dial_success graph: %w", err)
	}

	countriesInstant, err := statsInstant(ctx, tx, "country_name")
	if err != nil {
		return nil, fmt.Errorf("countries instant: %w", err)
	}

	return &StatsResult{
		Buckets:            timestamps,
		ClientNamesGraph:   clientNameGraph,
		DialSuccessGraph:   dialSuccessGraph,
		ClientNamesInstant: clientNameInstant,
		CountriesInstant:   countriesInstant,
		OSArchInstant:      nil,
	}, nil
}
