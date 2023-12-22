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

	row := db.db.QueryRowContext(
		ctx,
		`
			SELECT
				disc.node_id,
				disc.node_pubkey,
				disc.node_type,
				disc.first_found,
				disc.last_found,
				crawled.updated_at,
				disc.node_record,
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
				crawled.network_id,
				fork_id,
				next_fork_id,
				head_hash,
				blocks.timestamp,
				disc.ip_address,
				connection_type,
				country,
				city,
				latitude,
				longitude,
				next_crawl,
				EXISTS (
					SELECT 1
					FROM crawl_history history
					WHERE
						history.node_id = crawled.node_id
						AND history.direction = 'dial'
						AND (
							history.crawled_at > unixepoch('now', '-7 days')
							AND (
								history.error IS NULL
								OR history.error IN (  -- Disconnect Reasons
									'disconnect requested',
									'network error',
									'breach of protocol',
									'useless peer',
									'too many peers',
									'already connected',
									'incompatible p2p protocol version',
									'invalid node identity',
									'client quitting',
									'unexpected identity',
									'connected to self',
									'read timeout',
									'subprotocol error'
								)
							)
						)
				) dial_success
			FROM discovered_nodes AS disc
			LEFT JOIN crawled_nodes AS crawled ON (disc.node_id = crawled.node_id)
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE disc.node_id = ?;
		`,
		nodeIDBytes,
	)

	nodePage := new(NodeTable)

	var firstFound, lastFound int64
	var updatedAtInt, headHashTimeInt, nextCrawlInt *int64
	var forkIDInt *uint32
	var nodeRecord []byte

	err = row.Scan(
		&nodePage.nodeID,
		&nodePage.nodePubKey,
		&nodePage.NodeType,
		&firstFound,
		&lastFound,
		&updatedAtInt,
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
		&forkIDInt,
		&nodePage.NextForkID,
		&nodePage.HeadHash,
		&headHashTimeInt,
		&nodePage.IP,
		&nodePage.ConnectionType,
		&nodePage.Country,
		&nodePage.City,
		&nodePage.Latitude,
		&nodePage.Longitude,
		&nextCrawlInt,
		&nodePage.DialSuccess,
	)
	if err != nil {
		return nil, fmt.Errorf("row scan failed: %w", err)
	}

	nodePage.firstFound = time.Unix(firstFound, 0)
	nodePage.lastFound = time.Unix(lastFound, 0)
	nodePage.updatedAt = int64PrtToTimePtr(updatedAtInt)
	nodePage.HeadHashTime = int64PrtToTimePtr(headHashTimeInt)
	nodePage.nextCrawl = int64PrtToTimePtr(nextCrawlInt)

	if forkIDInt != nil {
		fid := common.Uint32ToForkID(*forkIDInt)
		nodePage.ForkID = &fid
	}

	record, err := common.LoadENR(nodeRecord)
	if err != nil {
		return nil, fmt.Errorf("loading node record failed: %w", err)
	}

	nodePage.NodeRecord = record

	rows, err := db.db.QueryContext(
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
					coalesce(error, '') AS error,
					row_number() OVER (
						PARTITION BY direction
						ORDER BY crawled_at DESC
					) AS row
				FROM crawl_history
				WHERE
					node_id = ?
				ORDER BY crawled_at DESC
			)
			WHERE row <= 10
		`,
		nodeIDBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("history query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		history := NodeTableHistory{} //nolint:exhaustruct
		var crawledAtInt int64

		err = rows.Scan(
			&crawledAtInt,
			&history.Direction,
			&history.Error,
		)
		if err != nil {
			return nil, fmt.Errorf("history row scan failed: %w", err)
		}

		history.CrawledAt = time.Unix(crawledAtInt, 0)

		if history.Direction == common.DirectionAccept {
			nodePage.HistoryAccept = append(nodePage.HistoryAccept, history)
		} else {
			nodePage.HistoryDial = append(nodePage.HistoryDial, history)
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("history rows iteration failed: %w", err)
	}

	return nodePage, nil
}

type NodeListQuery struct {
	Query       string
	IP          string
	NodeIDStart []byte
	NodeIDEnd   []byte
}

var maxNodeID = bytes.Repeat([]byte{0xff}, 32)

func ParseNodeListQuery(query string) (*NodeListQuery, error) {
	queryIP := ""
	var nodeIDStart []byte = nil
	var nodeIDEnd []byte = nil

	if query != "" {
		ip := net.ParseIP(query)

		if ip != nil {
			queryIP = query
		} else {
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
		IP:          queryIP,
		NodeIDStart: nodeIDStart,
		NodeIDEnd:   nodeIDEnd,
	}, nil
}

func (db *DB) GetNodeList(
	ctx context.Context,
	pageNumber int,
	networkID int64,
	synced int,
	query NodeListQuery,
	clientName string,
	clientUserData string,
	nodeType int,
) (*NodeList, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_node_list", start, err)

	pageSize := 20
	offset := (pageNumber - 1) * pageSize

	hint := ""
	if query.IP != "" {
		hint = "INDEXED BY discovered_nodes_ip_address_node_id"
	}

	rows, err := db.db.QueryContext(
		ctx,
		fmt.Sprintf(`
			SELECT
				disc.node_id,
				disc.node_pubkey,
				disc.node_type,
				crawled.updated_at,
				crawled.client_name,
				crawled.client_user_data,
				crawled.client_version,
				crawled.client_build,
				crawled.client_os,
				crawled.client_arch,
				crawled.country,
				blocks.timestamp,
				EXISTS (
					SELECT 1
					FROM crawl_history history
					WHERE
						history.node_id = crawled.node_id
						AND history.direction = 'dial'
						AND (
							history.crawled_at > unixepoch('now', '-7 days')
							AND (
								history.error IS NULL
								OR history.error IN (  -- Disconnect Reasons
									'disconnect requested',
									'network error',
									'breach of protocol',
									'useless peer',
									'too many peers',
									'already connected',
									'incompatible p2p protocol version',
									'invalid node identity',
									'client quitting',
									'unexpected identity',
									'connected to self',
									'read timeout',
									'subprotocol error'
								)
							)
						)
				) dial_success
			FROM discovered_nodes AS disc
				%s
			LEFT JOIN crawled_nodes AS crawled ON (
				disc.node_id = crawled.node_id
			)
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE
				(      -- Network ID filter
					?1 = -1
					OR crawled.network_id = ?1
				)
				AND (  -- Synced filter
					?2 = -1  -- All
					OR (     -- Not synced
						?2 = 0
						AND (
							blocks.timestamp IS NULL
							OR abs(crawled.updated_at - blocks.timestamp) >= 60
						)
					)
					OR (     -- Synced
						?2 = 1
						AND abs(crawled.updated_at - blocks.timestamp) < 60
					)
				)
				AND (  -- Node ID filter
					?3 IS NULL
					OR (disc.node_id >= ?3 AND disc.node_id <= ?4)
					OR (disc.node_pubkey >= ?3 AND disc.node_pubkey <= ?4)
				)
				AND (  -- IP address filter
					?5 = ''
					OR disc.ip_address = ?5
				)
				AND (  -- Client Name filter
					?6 = ''
					OR crawled.client_name = LOWER(?6)
				)
				AND (
					?7 = ''
					OR crawled.client_user_data = LOWER(?7)
				)
				AND (
					?8 = -1
					OR disc.node_type = ?8
				)
			ORDER BY disc.node_id
			LIMIT ?9 + 1
			OFFSET ?10
		`, hint),
		networkID,
		synced,
		query.NodeIDStart,
		query.NodeIDEnd,
		query.IP,
		clientName,
		clientUserData,
		nodeType,
		pageSize,
		offset,
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
		var updatedAtInt, headHashTimeInt *int64
		var userData *string

		err = rows.Scan(
			&row.nodeID,
			&row.nodePubKey,
			&row.NodeType,
			&updatedAtInt,
			&row.ClientName,
			&userData,
			&row.ClientVersion,
			&row.ClientBuild,
			&row.ClientOS,
			&row.ClientArch,
			&row.Country,
			&headHashTimeInt,
			&row.DialSuccess,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}

		row.UpdatedAt = int64PrtToTimePtr(updatedAtInt)
		row.HeadHashTimestamp = int64PrtToTimePtr(headHashTimeInt)

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
	Key    string
	Totals []*float64
}

type StatsGraph []StatsGraphSeries

type StatsSeriesInstant struct {
	Key   string
	Total int64
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
			Name:      series.Key,
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
					SUM(total) total
				FROM timeseries
				JOIN client.client_names USING (client_name_id)
				JOIN client.client_versions USING (client_version_id)
				JOIN client.os USING (client_os_id)
				JOIN client_arch USING (client_arch_id)
				JOIN geoname.countries USING (country_geoname_id)
				WHERE
					bucket = MAX(SELECT bucket FROM timeseries)
				GROUP BY
					key,
					total
				HAVING total IS NOT NULL
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
			&row.Key,
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
				SELECT
					key,
					array_agg(total) totals
				FROM (
					SELECT
						%s key,
						SUM(total) total
					FROM timeseries
					JOIN client.client_names USING (client_name_id)
					JOIN client.client_versions USING (client_version_id)
					GROUP BY
						bucket,
						key
				)
				GROUP BY key
				ORDER BY totals[array_lenth(totals)] DESC NULLS LAST  -- Sort by last element
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
			&series.Key,
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
	clientName string,
) (*StatsResult, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_stats", start, err)

	tx, err := db.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(
		ctx,
		`
			SELECT
				time_bucket_gapfill(
					GREATEST($3::TIMESTAMPTZ - $2::TIMESTAMP / 64, '30 minutes'),
					timestamp,
					$2,
					$3
				) bucket,
				client_name_id,
				client_version_id,
				client_os_id,
				client_arch_id,
				network_id,
				fork_id,
				next_fork_id,
				country_geoname_id,
				synced,
				dial_success,
				avg(total)::INT total
			INTO TEMPORARY UNLOGGED TABLE timeseries
			FROM stats.execution_nodes
			JOIN client.client_names USING (client_name_id)
			WHERE
				client_type = $1
				AND timestamp >= $2
				AND timestamp < $3
				AND (
					$4 = -1
					OR network_id = $4
				)
				AND (
					$5 = -1
					OR synced = ($5 = 1)
				)
				AND (
					$6 = ''
					OR client_names.client_name = $6
				)
			GROUP BY
				bucket,
				client_name_id,
				client_version_id,
				client_os_id,
				client_arch_id,
				network_id,
				fork_id,
				next_fork_id,
				country_geoname_id,
				synced,
				dial_success
			ORDER BY
				bucket ASC
		`,
		common.NodeTypeExecution,
		after,
		before,
		networkID,
		synced,
		clientName,
	)
	if err != nil {
		return nil, fmt.Errorf("create temp table: %w", err)
	}

	rows, err := tx.Query(ctx, `SELECT bucket FROM timeseries`)
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

	if clientName == "" {
		clientNameGraph, err = statsGraph(ctx, tx, "client_name")
		if err != nil {
			return nil, fmt.Errorf("client_name graph: %w", err)
		}
	} else {
		clientNameGraph, err = statsGraph(ctx, tx, "client_version")
		if err != nil {
			return nil, fmt.Errorf("client_version graph: %w", err)
		}
	}

	dialSuccessGraph, err := statsGraph(ctx, tx, "dial_success")
	if err != nil {
		return nil, fmt.Errorf("dial_success graph: %w", err)
	}

	clientNameInstant, err := statsInstant(ctx, tx, "client_name")
	if err != nil {
		return nil, fmt.Errorf("client_name instant: %w", err)
	}

	countriesInstant, err := statsInstant(ctx, tx, "country_name")
	if err != nil {
		return nil, fmt.Errorf("countries instant: %w", err)
	}

	osArchInstant, err := statsInstant(
		ctx,
		tx,
		("COALESCE(client_os_name, 'Unknown') || " +
			"' / ' || COALESCE(client_arch_name, 'Unknown')"),
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
