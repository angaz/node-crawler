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
	return denormalizeClient(
		ctx,
		tx,
		sqlite,
		"client_version",
		"client.versions",
		"stats",
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

func copySqliteToPGIdentifiers(ctx context.Context, tx pgx.Tx, sqlite *sql.DB) (map[string]int64, error) {
	slog.Info("migration start", "name", "client_identifiers")
	start := time.Now()
	defer func() {
		slog.Info("migration end", "name", "client_identifiers", "duration", time.Since(start))
	}()

	rows, err := sqlite.Query(
		`
			SELECT DISTINCT
				client_identifier
			FROM main.crawled_nodes
			WHERE client_identifier IS NOT NULL
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	err = ClientUpsertStrings(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("client upsert strings: %w", err)
	}

	idMap := make(map[string]int64)

	for rows.Next() {
		var identifier string

		err = rows.Scan(&identifier)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		client := common.ParseClientID(&identifier).Deref()

		row := tx.QueryRow(
			ctx,
			`
				SELECT
					client_identifier_id
				FROM client.upsert(
					client_identifier	=> nullif(@client_identifier, 'Unknown'),
					client_name			=> nullif(@client_name, 'Unknown'),
					client_user_data	=> nullif(@client_user_data, 'Unknown'),
					client_version		=> nullif(@client_version, 'Unknown'),
					client_build		=> nullif(@client_build, 'Unknown'),
					client_os			=> @client_os::client.os,
					client_arch			=> @client_arch::client.arch,
					client_language		=> nullif(@client_language, 'Unknown')
				)
			`,
			pgx.NamedArgs{
				"client_identifier": identifier,
				"client_name":       client.Name,
				"client_user_data":  client.UserData,
				"client_version":    client.Version,
				"client_build":      client.Build,
				"client_os":         client.OS,
				"client_arch":       client.Arch,
				"client_language":   client.Language,
			},
		)

		var identifierID int64

		err = row.Scan(&identifierID)
		if err != nil {
			return nil, fmt.Errorf("client.upsert scan: %w", err)
		}

		idMap[identifier] = identifierID
	}

	return idMap, nil
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

type location struct {
	country          string
	countryGeoNameID uint
	city             string
	cityGeoNameID    uint
	latitude         float64
	longitude        float64
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
		"North Korea":    1873107,
		"Turkey":         298795,
	}

	extraCities := []location{
		{
			country:          "North Korea",
			countryGeoNameID: 1873107,
			city:             "Pyongyang",
			cityGeoNameID:    1871859,
			latitude:         39.0365,
			longitude:        125.7611,
		},
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

	for _, city := range extraCities {
		_, err = tx.Exec(
			ctx,
			stmt.Name,

			city.countryGeoNameID,
			city.country,
			city.cityGeoNameID,
			city.city,
			city.latitude,
			city.longitude,
		)
		if err != nil {
			return nil, fmt.Errorf("exec: %w", err)
		}
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
			ORDER BY
				timestamp,
				network_id
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
			ORDER BY
				node_id
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

		cID := clientIdentifiersMap[clientIdentifier]
		capID := capabilitiesMap[capabilities]

		return []any{
			nodeID,
			cID,
			rlpxVersion,
			capID,
			networkID,
			forkID,
			nextForkID,
			headHash,
		}, nil
	})

	_, err = tx.CopyFrom(ctx, []string{"execution", "nodes"}, []string{
		"node_id",
		"client_identifier_id",
		"rlpx_version",
		"capabilities_id",
		"network_id",
		"fork_id",
		"next_fork_id",
		"head_hash",
	}, copier)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return nil
}

type discNode struct {
	NodeID        []byte
	NodeType      common.NodeType
	FirstFound    time.Time
	NodePubkey    []byte
	NodeRecord    []byte
	IPAddress     string
	CityGeonameID uint
}

type nextDiscCrawl struct {
	NodeID    []byte
	LastFound time.Time
	NextCrawl time.Time
}

type nextNodeCrawl struct {
	NodeID    []byte
	UpdatedAt *time.Time
	NextCrawl time.Time
	NodeType  common.NodeType
}

func randomHour() time.Duration {
	secs := rand.Int63n(3600)

	return time.Duration(secs) * time.Second
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
				disc.ip_address,
				first_found,
				last_found,
				next_crawl,
				updated_at
			FROM discovered_nodes disc
			LEFT JOIN crawled_nodes USING (node_id)
			ORDER BY
				node_id
		`,
	)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	discNodes := make(chan discNode, 8_000_000)
	nextDiscCrawls := make(chan nextDiscCrawl, 8_000_000)
	nextNodeCrawls := make(chan nextNodeCrawl, 8_000_000)

	for rows.Next() {
		var nodeID []byte
		var nodePubkey []byte
		var nodeType common.NodeType
		var nodeRecord []byte
		var ipAddress string
		var firstFound int64
		var lastFound int64
		var nextCrawl int64
		var updatedAt *int64

		err := rows.Scan(
			&nodeID,
			&nodePubkey,
			&nodeType,
			&nodeRecord,
			&ipAddress,
			&firstFound,
			&lastFound,
			&nextCrawl,
			&updatedAt,
		)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		ipAddr := net.ParseIP(ipAddress)
		if ipAddr == nil {
			return fmt.Errorf("parse ip: %s", ipAddress)
		}

		city, err := geoip.City(ipAddr)
		if err != nil {
			return fmt.Errorf("lookup ip: %w", err)
		}

		discNodes <- discNode{
			NodeID:        nodeID,
			NodeType:      nodeType,
			FirstFound:    time.Unix(firstFound, 0),
			NodePubkey:    nodePubkey,
			NodeRecord:    nodeRecord,
			IPAddress:     ipAddress,
			CityGeonameID: city.City.GeoNameID,
		}

		nextDiscCrawls <- nextDiscCrawl{
			NodeID:    nodeID,
			LastFound: time.Unix(lastFound, 0),
			NextCrawl: time.Unix(nextCrawl, 0),
		}

		var updatedAtTime *time.Time

		if updatedAt != nil {
			t := time.Unix(*updatedAt, 0)
			updatedAtTime = &t
		}

		nextNodeCrawls <- nextNodeCrawl{
			NodeID:    nodeID,
			UpdatedAt: updatedAtTime,
			NextCrawl: time.Now().UTC().Add(randomHour()),
			NodeType:  nodeType,
		}
	}

	close(discNodes)
	close(nextDiscCrawls)
	close(nextNodeCrawls)

	_, err = tx.CopyFrom(ctx, []string{"disc", "nodes"}, []string{
		"node_id",
		"node_type",
		"first_found",
		"node_pubkey",
		"node_record",
		"ip_address",
		"city_geoname_id",
	}, pgx.CopyFromFunc(func() ([]any, error) {
		for node := range discNodes {
			return []any{
				node.NodeID,
				node.NodeType.String(),
				node.FirstFound,
				node.NodePubkey,
				node.NodeRecord,
				node.IPAddress,
				node.CityGeonameID,
			}, nil
		}

		return nil, nil
	}))
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	_, err = tx.CopyFrom(ctx, []string{"crawler", "next_disc_crawl"}, []string{
		"node_id",
		"last_found",
		"next_crawl",
	}, pgx.CopyFromFunc(func() (row []any, err error) {
		for node := range nextDiscCrawls {
			return []any{
				node.NodeID,
				node.LastFound,
				node.NextCrawl,
			}, nil
		}

		return nil, nil
	}))
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	_, err = tx.CopyFrom(ctx, []string{"crawler", "next_node_crawl"}, []string{
		"node_id",
		"updated_at",
		"next_crawl",
		"node_type",
	}, pgx.CopyFromFunc(func() (row []any, err error) {
		for node := range nextNodeCrawls {
			return []any{
				node.NodeID,
				node.UpdatedAt,
				node.NextCrawl,
				node.NodeType.String(),
			}, nil
		}

		return nil, nil
	}))
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
			ORDER BY
				block_hash,
				network_id,
				timestamp,
				block_number
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
			ORDER BY
				node_id,
				crawled_at,
				direction,
				error
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

	clientIdentifiersMap, err := copySqliteToPGIdentifiers(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("client_identifiers: %w", err)
	}

	capabilitiesMap, err := copySqliteToPGCapabilities(ctx, tx, sqlite)
	if err != nil {
		return fmt.Errorf("capabilities: %w", err)
	}

	countriesMap, err := copySqlitePGcountriesCities(ctx, tx, sqlite, geoip)
	if err != nil {
		return fmt.Errorf("countries cities: %w", err)
	}

	err = migrateStatsTable(
		ctx,
		tx,
		sqlite,
		clientNamesMap,
		clientUserDataMap,
		clientVersionsMap,
		countriesMap,
	)
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
