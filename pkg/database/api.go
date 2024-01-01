package database

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/jackc/pgx/v5"
)

func BytesToUnit32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func (db *DB) GetNodeTable(ctx context.Context, nodeID string) (*NodeTable, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_node_table", start, err)

	nodeIDBytes, err := hex.DecodeString(nodeID)
	if err != nil {
		return nil, fmt.Errorf("decoding node id failed: %w", err)
	}

	row := db.pg.QueryRow(
		ctx,
		`
			SELECT
				node_id,
				node_pubkey,
				node_type,
				first_found,
				last_found,
				updated_at,
				node_record,
				client_identifier,
				client_name,
				client_user_data,
				client_version,
				client_build,
				client_os,
				client_arch,
				client_language,
				rlpx_version,
				capabilities,
				network_id,
				fork_id,
				fork_name,
				next_fork_id,
				next_fork_name,
				head_hash,
				timestamp,
				ip_address,
				country_name,
				city_name,
				latitude,
				longitude,
				next_crawl,
				dial_success
			FROM execution.node_view
			WHERE node_id = $1
		`,
		nodeIDBytes,
	)

	nodePage := new(NodeTable)

	var nodeRecord []byte

	err = row.Scan(
		&nodePage.nodeID,
		&nodePage.nodePubKey,
		&nodePage.NodeType,
		&nodePage.firstFound,
		&nodePage.lastFound,
		&nodePage.updatedAt,
		&nodeRecord,
		&nodePage.ClientID,
		&nodePage.ClientName,
		&nodePage.ClientUserData,
		&nodePage.ClientVersion,
		&nodePage.ClientBuild,
		&nodePage.ClientOS,
		&nodePage.ClientArch,
		&nodePage.ClientLanguage,
		&nodePage.RlpxVersion,
		&nodePage.Capabilities,
		&nodePage.networkID,
		&nodePage.ForkID,
		&nodePage.ForkName,
		&nodePage.NextForkID,
		&nodePage.NextForkName,
		&nodePage.HeadHash,
		&nodePage.HeadHashTime,
		&nodePage.IP,
		&nodePage.Country,
		&nodePage.City,
		&nodePage.Latitude,
		&nodePage.Longitude,
		&nodePage.nextCrawl,
		&nodePage.DialSuccess,
	)
	if err != nil {
		return nil, fmt.Errorf("row scan: %w", err)
	}

	record, err := common.LoadENR(nodeRecord)
	if err != nil {
		return nil, fmt.Errorf("loading node record: %w", err)
	}

	nodePage.NodeRecord = record

	rows, err := db.pg.Query(
		ctx,
		`
			SELECT
				crawled_at,
				direction,
				error
			FROM (
				SELECT
					crawled_at,
					direction,
					error,
					row_number() OVER (
						PARTITION BY direction
						ORDER BY crawled_at DESC
					) AS row
				FROM crawler.history
				WHERE
					node_id = $1
				ORDER BY crawled_at DESC
			)
			WHERE row <= 10
		`,
		nodeIDBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("history query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var history NodeTableHistory

		err = rows.Scan(
			&history.CrawledAt,
			&history.Direction,
			&history.Error,
		)
		if err != nil {
			return nil, fmt.Errorf("history row scan: %w", err)
		}

		if history.Direction == common.DirectionAccept {
			nodePage.HistoryAccept = append(nodePage.HistoryAccept, history)
		} else {
			nodePage.HistoryDial = append(nodePage.HistoryDial, history)
		}
	}

	rows.Close()

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("history rows iteration: %w", err)
	}

	return nodePage, nil
}

type NodeListQuery struct {
	Query       string
	IP          net.IP
	NodeIDStart []byte
	NodeIDEnd   []byte
}

var maxNodeID = bytes.Repeat([]byte{0xff}, 32)

func ParseNodeListQuery(query string) (*NodeListQuery, error) {
	var ip net.IP
	var nodeIDStart []byte = nil
	var nodeIDEnd []byte = nil

	if query != "" {
		ip = net.ParseIP(query)

		if ip == nil {
			nodeIDFilter := query

			if len(query)%2 == 1 {
				nodeIDFilter += "0"
			}

			queryBytes, err := hex.DecodeString(nodeIDFilter)
			if err != nil {
				return nil, fmt.Errorf("hex decoding query failed: %w", err)
			}

			nodeIDStart = queryBytes

			// If we had an odd number of digits in the query,
			// OR the last byte with 0x0f
			// Example:
			//   query = 4
			//   start = 0x40
			//   end   = 0x4f
			//
			// else, query length was even,
			// append 0xff to the node id end
			// Example:
			//   query = 40
			//   start = 0x40
			//   end   = 0x40ff
			if len(query)%2 == 1 {
				nodeIDEnd = bytes.Clone(queryBytes)
				nodeIDEnd[len(nodeIDEnd)-1] |= 0x0f
			} else {
				nodeIDEnd = append(queryBytes, 0xff)
			}
		}
	}

	return &NodeListQuery{
		Query:       query,
		IP:          ip,
		NodeIDStart: nodeIDStart,
		NodeIDEnd:   nodeIDEnd,
	}, nil
}

type EphemeryNetwork struct {
	Name      string
	NetworkID int64
}

func (db *DB) EphemeryNetworks(ctx context.Context) ([]EphemeryNetwork, error) {
	var err error

	defer metrics.ObserveDBQuery("get_ephemery_networks", time.Now(), err)

	rows, err := db.pg.Query(
		ctx,
		`
			SELECT
				network_name,
				network_id
			FROM network.ephemery_releases
			ORDER BY timestamp DESC
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	networks, err := pgx.CollectRows[EphemeryNetwork](rows, func(row pgx.CollectableRow) (EphemeryNetwork, error) {
		var network EphemeryNetwork

		err := row.Scan(
			&network.Name,
			&network.NetworkID,
		)
		if err != nil {
			return network, fmt.Errorf("scan: %w", err)
		}

		return network, nil
	})
	if err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	return networks, nil
}

func (db *DB) GetNodeList(
	ctx context.Context,
	pageNumber int,
	networkID int64,
	synced int,
	query NodeListQuery,
	clientName string,
	clientUserData string,
	nodeType *string,
) (*NodeList, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_node_list", start, err)

	pageSize := 20
	offset := (pageNumber - 1) * pageSize

	rows, err := db.pg.Query(
		ctx,
		`
			SELECT
				node_id,
				node_pubkey,
				node_type,
				updated_at,
				client_name,
				client_user_data,
				client_version,
				client_build,
				client_os,
				client_arch,
				country_name,
				timestamp,
				dial_success
			FROM execution.node_view
			WHERE
				(      -- Network ID filter
					@network_id = -1
					OR network_id = @network_id
				)
				AND (  -- Synced filter
					CASE
						WHEN @synced = -1 THEN
							TRUE
						WHEN @synced = 0 THEN
							synced IS FALSE
						WHEN @synced = 1 THEN
							synced IS TRUE
					END
				)
				AND (  -- Node ID filter
					@node_id_start::BYTEA IS NULL
					OR (node_id >= @node_id_start AND node_id <= @node_id_end)
					OR (node_pubkey >= @node_id_start AND node_pubkey <= @node_id_end)
				)
				AND (  -- IP address filter
					@ip_address::INET IS NULL
					OR ip_address = @ip_address
				)
				AND (  -- Client Name filter
					@client_name = ''
					OR client_name = LOWER(@client_name)
				)
				AND (
					@client_user_data = ''
					OR client_user_data = LOWER(@client_user_data)
				)
				AND (
					@node_type::TEXT IS NULL
					OR node_type = @node_type::client.node_type
				)
			ORDER BY node_id
			LIMIT @limit + 1
			OFFSET @offset
		`,
		pgx.NamedArgs{
			"network_id":       networkID,
			"synced":           synced,
			"node_id_start":    query.NodeIDStart,
			"node_id_end":      query.NodeIDEnd,
			"ip_address":       query.IP,
			"client_name":      clientName,
			"client_user_data": clientUserData,
			"node_type":        nodeType,
			"limit":            pageSize,
			"offset":           offset,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	out := NodeList{
		PageSize:       pageSize,
		PageNumber:     pageNumber,
		HasNextPage:    false,
		Synced:         synced,
		Offset:         offset,
		List:           []NodeListRow{},
		NetworkFilter:  networkID,
		Query:          query.Query,
		ClientName:     clientName,
		ClientUserData: clientUserData,
	}

	rowNumber := 0
	for rows.Next() {
		rowNumber++

		// We added 1 to the LIMIT to see if there were any more rows for
		// a next page. This is where we test for that.
		if rowNumber > pageSize {
			out.HasNextPage = true

			break
		}

		row := NodeListRow{} //nolint:exhaustruct
		var userData *string

		err = rows.Scan(
			&row.nodeID,
			&row.nodePubKey,
			&row.NodeType,
			&row.UpdatedAt,
			&row.ClientName,
			&userData,
			&row.ClientVersion,
			&row.ClientBuild,
			&row.ClientOS,
			&row.ClientArch,
			&row.Country,
			&row.HeadHashTimestamp,
			&row.DialSuccess,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}

		if row.ClientName != nil && userData != nil {
			newName := *row.ClientName + "/" + *userData
			row.ClientName = &newName
		}

		out.List = append(out.List, row)
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows failed: %w", err)
	}

	return &out, nil
}

type StatsGraphSeries struct {
	key    *string
	Totals []*float64
}

func (s StatsGraphSeries) Key() string {
	if s.key == nil {
		return common.Unknown
	}

	return *s.key
}

type StatsGraph []StatsGraphSeries

type StatsSeriesInstant struct {
	key   *string
	Total int64
}

func (i StatsSeriesInstant) Key() string {
	if i.key == nil {
		return common.Unknown
	}

	return *i.key
}

type StatsInstant struct {
	Series []StatsSeriesInstant
	Total  int64
}

type StatsResult struct {
	Buckets            []time.Time
	ClientNamesGraph   []StatsGraphSeries
	DialSuccessGraph   []StatsGraphSeries
	ClientNamesInstant []StatsSeriesInstant
	CountriesInstant   []StatsSeriesInstant
	OSArchInstant      []StatsSeriesInstant
}

func ToInstant(series []StatsSeriesInstant) StatsInstant {
	var total int64

	for _, series := range series {
		total += series.Total
	}

	return StatsInstant{
		Series: series,
		Total:  total,
	}
}

func (stats StatsResult) toTimeseries(series []StatsGraphSeries) Timeseries {
	times := make([]string, len(stats.Buckets))
	legend := make([]string, len(series))
	chartSeries := make([]ChartSeries, len(series))

	for i, ts := range stats.Buckets {
		times[i] = ts.UTC().Format("2006-01-02 15:04")
	}

	for i, series := range series {
		chartSeries[i] = ChartSeries{
			Name:      series.Key(),
			Type:      "line",
			Colour:    "",
			Stack:     "Total",
			AreaStyle: struct{}{},
			Emphasis: ChartSeriesEmphasis{
				Focus: "series",
			},
			Data: series.Totals,
		}
	}

	return Timeseries{
		Legend: legend,
		Series: chartSeries,
		XAxis: []ChartXAxis{
			{
				Type:       "category",
				BoundryGap: false,
				Data:       times,
			},
		},
		YAxisMax: nil,
	}
}

func (stats StatsResult) ClientNamesTimeseries() Timeseries {
	return stats.toTimeseries(stats.ClientNamesGraph)
}

func (stats StatsResult) DialSuccessTimeseries() Timeseries {
	return stats.toTimeseries(stats.DialSuccessGraph)
}

// !!! `column` IS NOT SANITIZED !!! Do not use user-provided values.
func statsInstant(
	ctx context.Context,
	tx pgx.Tx,
	key string,
) ([]StatsSeriesInstant, error) {
	rows, err := tx.Query(
		ctx,
		fmt.Sprintf(
			`
				SELECT
					%s key,
					SUM(total)::INTEGER total
				FROM timeseries
				LEFT JOIN client.names USING (client_name_id)
				LEFT JOIN client.versions USING (client_version_id)
				LEFT JOIN geoname.countries USING (country_geoname_id)
				WHERE
					bucket = (SELECT MAX(bucket) FROM timeseries WHERE total IS NOT NULL)
					AND total IS NOT NULL
				GROUP BY
					key
				ORDER BY total DESC
			`,
			key,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	stats := make([]StatsSeriesInstant, 0, 8)

	for rows.Next() {
		var row StatsSeriesInstant

		err := rows.Scan(
			&row.key,
			&row.Total,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		stats = append(stats, row)
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	return stats, nil
}

// !!! `column` IS NOT SANITIZED !!! Do not use user-provided values.
func statsGraph(
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
						bucket = (SELECT MAX(bucket) FROM timeseries WHERE total IS NOT NULL)
					GROUP BY
						key
					ORDER BY total DESC
				), grouped AS (
					SELECT
						bucket,
						%[1]s key,
						SUM(total)::INTEGER total
					FROM timeseries
					LEFT JOIN client.names USING (client_name_id)
					LEFT JOIN client.versions USING (client_version_id)
					GROUP BY
						bucket,
						key
				)
				SELECT
					grouped.key,
					array_agg(grouped.total ORDER BY grouped.bucket) totals
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

	graph := make([]StatsGraphSeries, 0, 8)

	for rows.Next() {
		var series StatsGraphSeries

		err := rows.Scan(
			&series.key,
			&series.Totals,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		graph = append(graph, series)
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	return graph, nil
}

func (db *DB) GetStats(
	ctx context.Context,
	after time.Time,
	before time.Time,
	networkID int64,
	synced int,
	nextFork int,
	nextForkName string,
	clientName string,
	graphInterval time.Duration,
) (*StatsResult, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_stats", start, err)

	tx, err := db.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	intervalHours := int(graphInterval.Hours())

	_, err = tx.Exec(
		ctx,
		fmt.Sprintf(
			`
				SELECT
					time_bucket_gapfill(
						make_interval(hours => @interval),
						nodes.bucket,
						@after::TIMESTAMPTZ,
						@before::TIMESTAMPTZ
					) bucket,
					client_name_id,
					client_version_id,
					client_os,
					client_arch,
					nodes.network_id,
					nodes.fork_id,
					next_fork_id,
					country_geoname_id,
					synced,
					dial_success,
					avg(total) total
				INTO TEMPORARY TABLE timeseries
				FROM stats.execution_nodes_%dh nodes
				LEFT JOIN client.names USING (client_name_id)
				LEFT JOIN network.forks next_fork ON (
					nodes.network_id = next_fork.network_id
					AND nodes.next_fork_id = next_fork.block_time
				)
				WHERE
					-- If we are filtering by a network ID, and we have the network
					-- in the forks table, the fork ID should exist. If we don't
					-- have the network, keep the record.
					CASE
						WHEN @network_id = -1 THEN
							TRUE
						WHEN EXISTS (
							SELECT 1
							FROM network.forks
							WHERE forks.network_id = nodes.network_id
						) THEN
							EXISTS (
								SELECT 1
								FROM network.forks
								WHERE
									forks.network_id = nodes.network_id
									AND forks.fork_id = nodes.fork_id
								)
						ELSE
							TRUE
					END
					AND bucket >= @after::TIMESTAMPTZ
					AND bucket < @before::TIMESTAMPTZ
					AND (
						@network_id = -1
						OR nodes.network_id = @network_id
					)
					AND (
						@synced = -1
						OR synced = (@synced = 1)
					)
					AND (
						@client_name = ''
						OR names.client_name = @client_name
					)
					AND (
						@next_fork_name = ''
						OR next_fork.fork_name = @next_fork_name
					)
				GROUP BY
					1,
					client_name_id,
					client_version_id,
					client_os,
					client_arch,
					nodes.network_id,
					nodes.fork_id,
					next_fork_id,
					country_geoname_id,
					synced,
					dial_success
				ORDER BY
					1 ASC
			`,
			intervalHours,
		),
		pgx.NamedArgs{
			"interval":       intervalHours,
			"after":          after.Format(time.RFC3339),
			"before":         before.Format(time.RFC3339),
			"network_id":     networkID,
			"synced":         synced,
			"client_name":    clientName,
			"next_fork_name": "",
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create temp table: %w", err)
	}

	rows, err := tx.Query(ctx, `SELECT DISTINCT bucket FROM timeseries ORDER BY bucket`)
	if err != nil {
		return nil, fmt.Errorf("query buckets: %w", err)
	}
	defer rows.Close()

	buckets := make([]time.Time, 0, 64)

	for rows.Next() {
		var bucket time.Time

		err := rows.Scan(&bucket)
		if err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}

		buckets = append(buckets, bucket)
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows buckets: %w", err)
	}

	var clientNameGraph []StatsGraphSeries
	var clientNameInstant []StatsSeriesInstant

	if clientName == "" {
		clientNameGraph, err = statsGraph(ctx, tx, "client_name")
		if err != nil {
			return nil, fmt.Errorf("client_name graph: %w", err)
		}

		clientNameInstant, err = statsInstant(ctx, tx, "client_name")
		if err != nil {
			return nil, fmt.Errorf("client_name instant: %w", err)
		}
	} else {
		clientNameGraph, err = statsGraph(ctx, tx, "client_version")
		if err != nil {
			return nil, fmt.Errorf("client_version graph: %w", err)
		}

		clientNameInstant, err = statsInstant(ctx, tx, "client_version")
		if err != nil {
			return nil, fmt.Errorf("client_version instant: %w", err)
		}
	}

	dialSuccessGraph, err := statsGraph(ctx, tx, "CASE WHEN dial_success THEN 'Success' ELSE 'Fail' END")
	if err != nil {
		return nil, fmt.Errorf("dial_success graph: %w", err)
	}

	countriesInstant, err := statsInstant(ctx, tx, "country_name")
	if err != nil {
		return nil, fmt.Errorf("countries instant: %w", err)
	}

	osArchInstant, err := statsInstant(
		ctx,
		tx,
		("COALESCE(client_os, 'Unknown') || " +
			"' / ' || COALESCE(client_arch, 'Unknown')"),
	)
	if err != nil {
		return nil, fmt.Errorf("os/arch instant: %w", err)
	}

	return &StatsResult{
		Buckets:            buckets,
		ClientNamesGraph:   clientNameGraph,
		DialSuccessGraph:   dialSuccessGraph,
		ClientNamesInstant: clientNameInstant,
		CountriesInstant:   countriesInstant,
		OSArchInstant:      osArchInstant,
	}, nil
}
