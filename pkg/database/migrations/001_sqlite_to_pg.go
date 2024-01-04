package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"

	"log/slog"

	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/jackc/pgx/v5"
	"github.com/oschwald/geoip2-golang"
)

func randomString(prefix string) string {
	return fmt.Sprintf("%s_%x", prefix, rand.Uint64())
}

func denormalizeString(
	ctx context.Context,
	tx pgx.Tx,
	sqlite *sql.DB,
	selectSQL string,
	insertSQL string,
) (map[string]int64, error) {
	names := map[string]int64{}

	rows, err := sqlite.QueryContext(
		ctx,
		selectSQL,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	stmt, err := tx.Prepare(
		ctx,
		randomString("insert"),
		insertSQL,
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

		names[name] = id
	}

	return names, nil
}

func denormalizeClient(ctx context.Context, tx pgx.Tx, sqlite *sql.DB, column string, table string, schema string) (map[string]int64, error) {
	slog.Info("migration start", "name", column)
	start := time.Now()
	defer func() {
		slog.Info("migration end", "name", column, "duration", time.Since(start))
	}()

	return denormalizeString(
		ctx,
		tx,
		sqlite,
		fmt.Sprintf(`
			SELECT DISTINCT %[1]s
			FROM %[2]s.crawled_nodes
			WHERE %[1]s IS NOT NULL
		`, column, schema),
		fmt.Sprintf(`
			INSERT INTO %[2]s (
				%[1]s
			) VALUES (
				$1
			)
			RETURNING %[1]s_id
		`, column, table),
	)
}

func copySqliteToPGClientName(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	return denormalizeClient(
		ctx,
		tx,
		sqlite,
		"client_name",
		"client.names",
		"stats",
	)
}

func copySqliteToPGClientVersion(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	column := "client_version"
	table := "client.versions"
	schema := "stats"

	slog.Info("migration start", "name", column)
	start := time.Now()
	defer func() {
		slog.Info("migration end", "name", column, "duration", time.Since(start))
	}()

	return denormalizeString(
		ctx,
		tx,
		sqlite,
		fmt.Sprintf(`
			SELECT DISTINCT CASE
				WHEN client_name = 'reth' THEN
					CASE WHEN instr(client_build, '-') != 0 THEN
						client_version || '-' || substr(client_build, 0, instr(client_build, '-'))
					WHEN client_build IS NOT NULL AND client_build != '' THEN
						client_version || '-' || client_build
					ELSE
						client_version
					END
				ELSE
					client_version
			END
			FROM main.crawled_nodes
			WHERE %[1]s IS NOT NULL

			UNION

			SELECT DISTINCT client_version
			FROM stats.crawled_nodes
			WHERE %[1]s IS NOT NULL
		`, column, schema),
		fmt.Sprintf(`
			INSERT INTO %[2]s (
				%[1]s
			) VALUES (
				$1
			)
			RETURNING %[1]s_id
		`, column, table),
	)
}

func copySqliteToPGClientUserData(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	return denormalizeClient(
		ctx,
		tx,
		sqlite,
		"client_user_data",
		"client.user_data",
		"stats",
	)
}

func copySqliteToPGClientBuild(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	return denormalizeClient(
		ctx,
		tx,
		sqlite,
		"client_build",
		"client.builds",
		"main",
	)
}

func copySqliteToPGIdentifiers(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	return denormalizeClient(
		ctx,
		tx,
		sqlite,
		"client_identifier",
		"client.identifiers",
		"main",
	)
}

func copySqliteToPGLanguages(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	return denormalizeClient(
		ctx,
		tx,
		sqlite,
		"client_language",
		"client.languages",
		"main",
	)
}

func copySqliteToPGCapabilities(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	return denormalizeClient(
		ctx,
		tx,
		sqlite,
		"capabilities",
		"execution.capabilities",
		"main",
	)
}

func copySqlitePGcountriesCities(
	ctx context.Context,
	tx pgx.Tx,
	sqlite *sql.DB,
	geoip *geoip2.Reader,
) (map[string]int32, error) {
	slog.Info("migration start", "name", "cities")
	start := time.Now()
	defer func() {
		slog.Info("migration end", "name", "cities", "duration", time.Since(start))
	}()

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
				country_geoname_id,
				latitude,
				longitude
			) VALUES (
				$3,
				$4,
				$1,
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

func migrateStatsTable(
	ctx context.Context,
	tx pgx.Tx,
	sqlite *sql.DB,
	clientNamesMap map[string]int64,
	clientUserDataMap map[string]int64,
	clientVersionsMap map[string]int64,
	countriesMap map[string]int32,
) error {
	slog.Info("migration start", "name", "stats table")
	start := time.Now()
	defer func() {
		slog.Info("migration end", "name", "stats table", "duration", time.Since(start))
	}()

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
				coalesce(next_fork_id, 0),
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
		var nextForkID uint64
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

func migrateExecutionNodesTable(
	ctx context.Context,
	tx pgx.Tx,
	sqlite *sql.DB,
	clientIdentifiersMap map[string]int64,
	clientNamesMap map[string]int64,
	clientUserDataMap map[string]int64,
	clientVersionsMap map[string]int64,
	clientBuildsMap map[string]int64,
	clientLanguagesMap map[string]int64,
	capabilitiesMap map[string]int64,
) error {
	slog.Info("migration start", "name", "execution nodes table")
	start := time.Now()
	defer func() {
		slog.Info("migration end", "name", "execution nodes table", "duration", time.Since(start))
	}()

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT
				node_id,
				updated_at,
				client_identifier,
				rlpx_version,
				capabilities,
				network_id,
				fork_id,
				coalesce(next_fork_id, 0),
				head_hash
			FROM crawled_nodes
			WHERE
				client_identifier IS NOT NULL
				AND head_hash IS NOT NULL
				AND capabilities IS NOT NULL
				AND network_id IS NOT NULL
				AND fork_id IS NOT NULL
				AND rlpx_version IS NOT NULL
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

		var nodeID []byte
		var updatedAt int64
		var clientIdentifier string
		var rlpxVersion int64
		var capabilities string
		var networkID int64
		var forkID int64
		var nextForkID int64
		var headHash []byte

		err := rows.Scan(
			&nodeID,
			&updatedAt,
			&clientIdentifier,
			&rlpxVersion,
			&capabilities,
			&networkID,
			&forkID,
			&nextForkID,
			&headHash,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		var cnID, cudID, cvID, cbID, clID *int64
		var os common.OS
		var arch common.Arch

		cID := clientIdentifiersMap[clientIdentifier]
		capID := capabilitiesMap[capabilities]

		client := common.ParseClientID(&clientIdentifier)

		if client != nil {
			if client.Name != common.Unknown {
				id, ok := clientNamesMap[client.Name]
				if !ok {
					return nil, fmt.Errorf("client not found: %s, %s", client.Name, clientIdentifier)
				}
				cnID = &id
			}

			if client.UserData != common.Unknown {
				id, ok := clientUserDataMap[client.UserData]
				if !ok {
					return nil, fmt.Errorf("user data not found: %s, %s", client.UserData, clientIdentifier)
				}
				cudID = &id
			}

			if client.Version != common.Unknown {
				id, ok := clientVersionsMap[client.Version]
				if !ok {
					return nil, fmt.Errorf("version not found: %s, %s", client.Version, clientIdentifier)
				}
				cvID = &id
			}

			if client.Build != common.Unknown {
				id, ok := clientBuildsMap[client.Build]
				if !ok {
					return nil, fmt.Errorf("build not found: %s, %s", client.Build, clientIdentifier)
				}
				cbID = &id
			}

			if client.Language != common.Unknown {
				id, ok := clientLanguagesMap[client.Language]
				if !ok {
					return nil, fmt.Errorf("language not found: %s, %s", client.Language, clientIdentifier)
				}
				clID = &id
			}

			os = client.OS
			arch = client.Arch
		} else {
			os = common.OSUnknown
			arch = common.ArchUnknown
		}

		return []any{
			nodeID,
			time.Unix(updatedAt, 0),
			cID,
			rlpxVersion,
			capID,
			networkID,
			forkID,
			nextForkID,
			headHash,
			cnID,
			cudID,
			cvID,
			cbID,
			os.String(),
			arch.String(),
			clID,
		}, nil
	})

	_, err = tx.CopyFrom(ctx, []string{"execution", "nodes"}, []string{
		"node_id",
		"updated_at",
		"client_identifier_id",
		"rlpx_version",
		"capabilities_id",
		"network_id",
		"fork_id",
		"next_fork_id",
		"head_hash",
		"client_name_id",
		"client_user_data_id",
		"client_version_id",
		"client_build_id",
		"client_os",
		"client_arch",
		"client_language_id",
	}, copier)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}

func migrateDiscNodes(
	ctx context.Context,
	tx pgx.Tx,
	sqlite *sql.DB,
	geoip *geoip2.Reader,
) error {
	slog.Info("migration start", "name", "disc nodes table")
	start := time.Now()
	defer func() {
		slog.Info("migration end", "name", "disc nodes table", "duration", time.Since(start))
	}()

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT
				node_id,
				node_pubkey,
				node_type,
				node_record,
				ip_address,
				first_found,
				last_found,
				next_crawl
			FROM discovered_nodes
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

		var nodeID []byte
		var nodePubkey []byte
		var nodeType int64
		var nodeRecord []byte
		var ipAddress string
		var firstFound int64
		var lastFound int64
		var nextCrawl int64

		err := rows.Scan(
			&nodeID,
			&nodePubkey,
			&nodeType,
			&nodeRecord,
			&ipAddress,
			&firstFound,
			&lastFound,
			&nextCrawl,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		ipAddr := net.ParseIP(ipAddress)
		if ipAddr == nil {
			return nil, fmt.Errorf("parse ip: %s", ipAddress)
		}

		city, err := geoip.City(ipAddr)
		if err != nil {
			return nil, fmt.Errorf("lookup ip: %w", err)
		}

		return []any{
			nodeID,
			common.NodeType(nodeType).String(),
			time.Unix(firstFound, 0),
			time.Unix(lastFound, 0),
			time.Unix(nextCrawl, 0),
			nodePubkey,
			nodeRecord,
			ipAddress,
			city.City.GeoNameID,
		}, nil
	})

	_, err = tx.CopyFrom(ctx, []string{"disc", "nodes"}, []string{
		"node_id",
		"node_type",
		"first_found",
		"last_found",
		"next_crawl",
		"node_pubkey",
		"node_record",
		"ip_address",
		"city_geoname_id",
	}, copier)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}

func migrateBlocks(
	ctx context.Context,
	tx pgx.Tx,
	sqlite *sql.DB,
) error {
	slog.Info("migration start", "name", "blocks table")
	start := time.Now()
	defer func() {
		slog.Info("migration end", "name", "blocks table", "duration", time.Since(start))
	}()

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT
				block_hash,
				network_id,
				timestamp,
				block_number
			FROM blocks
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

		var blockHash []byte
		var networkID int64
		var timestamp int64
		var blockNumber int64

		err := rows.Scan(
			&blockHash,
			&networkID,
			&timestamp,
			&blockNumber,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		return []any{
			blockHash,
			networkID,
			time.Unix(timestamp, 0),
			blockNumber,
		}, nil
	})

	_, err = tx.CopyFrom(ctx, []string{"execution", "blocks"}, []string{
		"block_hash",
		"network_id",
		"timestamp",
		"block_number",
	}, copier)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}

func migrateCrawlHistory(
	ctx context.Context,
	tx pgx.Tx,
	sqlite *sql.DB,
) error {
	slog.Info("migration start", "name", "crawl history table")
	start := time.Now()
	defer func() {
		slog.Info("migration end", "name", "crawl history table", "duration", time.Since(start))
	}()

	rows, err := sqlite.QueryContext(
		ctx,
		`
			SELECT
				node_id,
				crawled_at,
				direction,
				error
			FROM crawl_history
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

		var nodeID []byte
		var crawledAt int64
		var direction string
		var error *string

		err := rows.Scan(
			&nodeID,
			&crawledAt,
			&direction,
			&error,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		if error != nil {
			if strings.Contains(*error, "protocol not available") {
				*error = "protocol not available"
			}
		}

		return []any{
			nodeID,
			time.Unix(crawledAt, 0),
			direction,
			error,
		}, nil
	})

	_, err = tx.CopyFrom(ctx, []string{"crawler", "history"}, []string{
		"node_id",
		"crawled_at",
		"direction",
		"error",
	}, copier)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}

func Migrate001SqliteToPG(ctx context.Context, tx pgx.Tx, sqlite *sql.DB, geoip *geoip2.Reader) error {
	clientIdentifiersMap, err := copySqliteToPGIdentifiers(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_identifiers: %w", err)
	}

	clientNamesMap, err := copySqliteToPGClientName(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_names: %w", err)
	}

	clientVersionsMap, err := copySqliteToPGClientVersion(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_versions: %w", err)
	}

	clientBuildsMap, err := copySqliteToPGClientBuild(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_builds: %w", err)
	}

	clientUserDataMap, err := copySqliteToPGClientUserData(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_user_data: %w", err)
	}

	clientLanguagesMap, err := copySqliteToPGLanguages(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_languages: %w", err)
	}

	capabilitiesMap, err := copySqliteToPGCapabilities(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("capabilities: %w", err)
	}

	countriesMap, err := copySqlitePGcountriesCities(ctx, tx, sqlite, geoip)
	if err != nil {
		return fmt.Errorf("countries cities: %w", err)
	}

	err = migrateStatsTable(ctx, tx, sqlite, clientNamesMap, clientUserDataMap, clientVersionsMap, countriesMap)
	if err != nil {
		return fmt.Errorf("migrate stats table: %w", err)
	}

	err = migrateDiscNodes(ctx, tx, sqlite, geoip)
	if err != nil {
		return fmt.Errorf("migrate disc nodes table: %w", err)
	}

	err = migrateExecutionNodesTable(
		ctx, tx, sqlite,
		clientIdentifiersMap,
		clientNamesMap,
		clientUserDataMap,
		clientVersionsMap,
		clientBuildsMap,
		clientLanguagesMap,
		capabilitiesMap,
	)
	if err != nil {
		return fmt.Errorf("migrate execution nodes table: %w", err)
	}

	err = migrateBlocks(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("migrate blocks: %w", err)
	}

	err = migrateCrawlHistory(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("migrate crawl history: %w", err)
	}

	return nil
}
