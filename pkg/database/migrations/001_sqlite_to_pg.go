package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oschwald/geoip2-golang"
)

func copySqliteToPGClientName(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	clientNames := map[string]int64{}

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT DISTINCT client_name
			FROM stats.crawled_nodes
			WHERE client_name IS NOT NULL
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	stmt, err := tx.Prepare(
		ctx,
		"insert_client_names",
		`
			INSERT INTO client.client_names (
				client_name
			) VALUES (
				$1
			)
			ON CONFLICT (client_name) DO NOTHING
			RETURNING client_name_id
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}

	for rows.Next() {
		var name string
		var id int64

		err = rows.Scan(&name)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		row := tx.QueryRow(
			ctx,
			stmt.Name,
			name,
		)

		err = row.Scan(&id)
		if err != nil {
			return nil, fmt.Errorf("insert: %w", err)
		}

		clientNames[name] = id
	}

	return clientNames, nil
}

func copySqliteToPGClientVersion(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	clientVersions := map[string]int64{}

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT DISTINCT client_version
			FROM stats.crawled_nodes
			WHERE client_version IS NOT NULL
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	stmt, err := tx.Prepare(
		ctx,
		"insert_client_version",
		`
			INSERT INTO client.client_versions (
				client_version
			) VALUES (
				$1
			)
			ON CONFLICT (client_version) DO NOTHING
			RETURNING client_version_id
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}

	for rows.Next() {
		var version string
		var id int64

		err = rows.Scan(&version)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		row := tx.QueryRow(
			ctx,
			stmt.Name,
			version,
		)

		err = row.Scan(&id)
		if err != nil {
			return nil, fmt.Errorf("insert: %w", err)
		}

		clientVersions[version] = id
	}

	return clientVersions, nil
}

func copySqliteToPGClientUserData(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	clientUserDatas := map[string]int64{}

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT DISTINCT client_user_data
			FROM stats.crawled_nodes
			WHERE client_user_data IS NOT NULL
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	stmt, err := tx.Prepare(
		ctx,
		"insert_client_user_data",
		`
			INSERT INTO client.client_user_data (
				client_user_data
			) VALUES (
				$1
			)
			ON CONFLICT (client_user_data) DO NOTHING
			RETURNING client_user_data_id
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}

	for rows.Next() {
		var userData string
		var id int64

		err = rows.Scan(&userData)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		row := tx.QueryRow(
			ctx,
			stmt.Name,
			userData,
		)

		err = row.Scan(&id)
		if err != nil {
			return nil, fmt.Errorf("insert: %w", err)
		}

		clientUserDatas[userData] = id
	}

	return clientUserDatas, nil
}

func copySqlitePGcountriesCities(
	ctx context.Context,
	tx pgx.Tx,
	sqlite *sql.DB,
	geoip *geoip2.Reader,
) (map[string]int32, error) {
	countriesMap := map[string]int32{
		"Congo Republic": 2260494,
		"Cuba":           3562981,
		"Eritrea":        338010,
		"Gabon":          2400553,
		"Netherlands":    2750405,
		"Turkey":         298795,
	}

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT
				DISTINCT ip_address
			FROM (
				SELECT ip_address
				FROM discovered_nodes

				UNION

				SELECT ip_address
				FROM crawled_nodes
			)
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	stmt, err := tx.Prepare(
		ctx,
		"insert_geoname_country_city",
		`
			WITH countries AS (
				INSERT INTO geoname.countries (
					country_geoname_id,
					country_name
				) VALUES (
					$1,
					$2
				) ON CONFLICT (country_geoname_id) DO NOTHING
			)

			INSERT INTO geoname.cities (
				city_geoname_id,
				city_name,
				latitude,
				longitude
			) VALUES (
				$3,
				$4,
				$5,
				$6
			) ON CONFLICT (city_geoname_id) DO NOTHING
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}

	for rows.Next() {
		var ipStr string

		err := rows.Scan(&ipStr)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		ip := net.ParseIP(ipStr)
		if ip == nil {
			return nil, fmt.Errorf("invalid IP: %s", ipStr)
		}

		city, err := geoip.City(ip)
		if err != nil {
			return nil, fmt.Errorf("ip lookup: %w", err)
		}

		countryGeonameID := city.Country.GeoNameID
		countryName := city.Country.Names["en"]

		_, err = tx.Exec(
			ctx,
			stmt.Name,

			countryGeonameID,
			countryName,
			city.City.GeoNameID,
			city.City.Names["en"],
			city.Location.Latitude,
			city.Location.Longitude,
		)
		if err != nil {
			return nil, fmt.Errorf("exec: %w", err)
		}

		countriesMap[countryName] = int32(countryGeonameID)
	}

	return countriesMap, nil
}

func Migrate001SqliteToPG(ctx context.Context, tx pgx.Tx, sqlite *sql.DB, geoip *geoip2.Reader) error {
	clientNamesMap, err := copySqliteToPGClientName(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_names: %w", err)
	}

	clientVersionsMap, err := copySqliteToPGClientVersion(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_versions: %w", err)
	}

	clientUserDataMap, err := copySqliteToPGClientUserData(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_user_data: %w", err)
	}

	countriesMap, err := copySqlitePGcountriesCities(ctx, tx, sqlite, geoip)
	if err != nil {
		return fmt.Errorf("countries cities: %w", err)
	}

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT
				timestamp,
				client_name,
				client_user_data,
				client_version,
				COALESCE(client_os, 'Unknown'),
				COALESCE(client_arch, 'Unknown'),
				network_id,
				fork_id,
				next_fork_id,
				country,
				synced,
				dial_success,
				total
			FROM stats.crawled_nodes
			WHERE country NOT IN ('Greenland')
		`,
	)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	copier := pgx.CopyFromFunc(func() ([]any, error) {
		if !rows.Next() {
			return nil, nil
		}

		var timestamp int64
		var clientName *string
		var clientUserData *string
		var clientVersion *string
		var clientOS string
		var clientArch string
		var networkID int64
		var forkID int64
		var nextForkID *uint64
		var country string
		var synced bool
		var dialSuccess bool
		var total int64

		err := rows.Scan(
			&timestamp,
			&clientName,
			&clientUserData,
			&clientVersion,
			&clientOS,
			&clientArch,
			&networkID,
			&forkID,
			&nextForkID,
			&country,
			&synced,
			&dialSuccess,
			&total,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		var cnID, cudID, cvID *int64

		if clientName != nil {
			id := clientNamesMap[*clientName]
			cnID = &id
		}

		if clientUserData != nil {
			id := clientUserDataMap[*clientUserData]
			cudID = &id
		}

		if clientVersion != nil {
			id := clientVersionsMap[*clientVersion]
			cvID = &id
		}

		countryGeonameID, ok := countriesMap[country]
		if !ok {
			return nil, fmt.Errorf("country not found: %s", country)
		}

		return []any{
			time.Unix(timestamp, 0),
			cnID,
			cudID,
			cvID,
			clientOS,
			clientArch,
			networkID,
			forkID,
			nextForkID,
			countryGeonameID,
			synced,
			dialSuccess,
			total,
		}, nil
	})

	_, err = tx.CopyFrom(ctx, []string{"stats", "execution_nodes"}, []string{
		"timestamp",
		"client_name_id",
		"client_user_data_id",
		"client_version_id",
		"client_os",
		"client_arch",
		"network_id",
		"fork_id",
		"next_fork_id",
		"country_geoname_id",
		"synced",
		"dial_success",
		"total",
	}, copier)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}
