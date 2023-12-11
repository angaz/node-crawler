package database

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
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
		fid := Uint32ToForkID(*forkIDInt)
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
			ORDER BY disc.node_id
			LIMIT ?8 + 1
			OFFSET ?9
		`, hint),
		networkID,
		synced,
		query.NodeIDStart,
		query.NodeIDEnd,
		query.IP,
		clientName,
		clientUserData,
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

func (db *DB) GetStats(
	ctx context.Context,
	after time.Time,
	before time.Time,
	networkID int64,
	synced int,
	clientName string,
) (AllStats, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_stats", start, err)

	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				timestamp,
				client_name,
				client_version,
				client_os,
				client_arch,
				network_id,
				fork_id,
				next_fork_id,
				country,
				synced,
				dial_success,
				total
			FROM stats.crawled_nodes
			WHERE
				timestamp > ?1
				AND timestamp <= ?2
				AND (
					?3 = -1
					OR network_id = ?3
				)
				AND (
					?4 = -1
					OR synced = ?4
				)
				AND (
					?5 = ''
					OR client_name = ?5
				)
			ORDER BY
				timestamp ASC
		`,
		after.Unix(),
		before.Unix(),
		networkID,
		synced,
		clientName,
	)
	if err != nil {
		return AllStats{}, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	allStats := make([]Stats, 0, 1024)

	for rows.Next() {
		var stats Stats
		var clientName, clientVersion, clientOS, clientArch *string
		var timestamp int64

		err := rows.Scan(
			&timestamp,
			&clientName,
			&clientVersion,
			&clientOS,
			&clientArch,
			&stats.NetworkID,
			&stats.ForkID,
			&stats.NextForkID,
			&stats.Country,
			&stats.Synced,
			&stats.DialSuccess,
			&stats.Total,
		)
		if err != nil {
			return AllStats{}, fmt.Errorf("scan failed: %w", err)
		}

		stats.Timestamp = time.Unix(timestamp, 0)

		stats.Client = newClient(
			clientName,
			nil,
			clientVersion,
			nil,
			clientOS,
			clientArch,
			nil,
		)
		allStats = append(allStats, stats)
	}

	return AllStats(allStats), nil
}

type HistoryListRow struct {
	NodeID           string
	ClientIdentifier *string
	NetworkID        *int64
	CrawledAt        time.Time
	Direction        string
	Error            *string
}

func (r HistoryListRow) CrawledAtStr() string {
	return r.CrawledAt.UTC().Format(time.RFC3339)
}

func (r HistoryListRow) SinceCrawled() string {
	return since(&r.CrawledAt)
}

func (r HistoryListRow) NetworkIDStr() string {
	if r.NetworkID == nil {
		return ""
	}

	return fmt.Sprintf("%s (%d)", NetworkName(r.NetworkID), *r.NetworkID)
}

type HistoryList struct {
	Rows      []HistoryListRow
	NetworkID int64
	IsError   int
	Before    *time.Time
	After     *time.Time
	LastTime  *time.Time
	FirstTime *time.Time
}

var DateTimeLocal = "2006-01-02T15:04:05"

func (l HistoryList) BeforeStr() string {
	if l.Before == nil {
		return ""
	}

	return l.Before.UTC().Format(DateTimeLocal)
}

func (l HistoryList) AfterStr() string {
	if l.After == nil {
		return ""
	}

	return l.After.UTC().Format(DateTimeLocal)
}

func (l HistoryList) FirstTimeStr() string {
	if l.FirstTime == nil {
		return l.AfterStr()
	}

	return l.FirstTime.UTC().Format(DateTimeLocal)
}

func (l HistoryList) LastTimeStr() string {
	if l.FirstTime == nil {
		return l.BeforeStr()
	}

	return l.LastTime.UTC().Format(DateTimeLocal)
}

func timePtrToUnixPtr(t *time.Time) *int64 {
	if t == nil {
		return nil
	}

	u := t.Unix()
	return &u
}

func (db *DB) GetHistoryList(
	ctx context.Context,
	before *time.Time,
	after *time.Time,
	networkID int64,
	isError int,
) (*HistoryList, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_history_list", start, err)

	queryOrderDirection := "ASC"
	if before == nil {
		queryOrderDirection = "DESC"
	}

	rows, err := db.db.QueryContext(
		ctx,
		// Don't ever do this, but we have no other choice because I could not
		// find another way to conditionally set the order direction. :(
		fmt.Sprintf(`
			SELECT
				history.node_id,
				crawled.client_identifier,
				crawled.network_id,
				history.crawled_at,
				history.direction,
				history.error
			FROM crawl_history AS history
			LEFT JOIN crawled_nodes AS crawled ON (history.node_id = crawled.node_id)
			WHERE
				(
					?1 IS NULL
					OR history.crawled_at >= ?1
				)
				AND (
					?2 IS NULL
					OR history.crawled_at <= ?2
				)
				AND (
					?3 = -1
					OR crawled.network_id = ?3
				)
				AND (
					?4 = -1
					OR (
						?4 = 0
						AND history.error IS NULL
					)
					OR (
						?4 = 1
						AND history.error IS NOT NULL
					)
				)
			ORDER BY history.crawled_at %s
			LIMIT 50
		`, queryOrderDirection),
		timePtrToUnixPtr(before),
		timePtrToUnixPtr(after),
		networkID,
		isError,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	historyList := HistoryList{
		Rows:      []HistoryListRow{},
		NetworkID: networkID,
		IsError:   isError,
		Before:    before,
		After:     after,
		FirstTime: nil,
		LastTime:  nil,
	}

	for rows.Next() {
		row := HistoryListRow{} //nolint:exhaustruct
		nodeIDBytes := make([]byte, 32)
		var crawledAtInt int64

		err := rows.Scan(
			&nodeIDBytes,
			&row.ClientIdentifier,
			&row.NetworkID,
			&crawledAtInt,
			&row.Direction,
			&row.Error,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		row.NodeID = hex.EncodeToString(nodeIDBytes[:])
		row.CrawledAt = time.Unix(crawledAtInt, 0)
		historyList.Rows = append(historyList.Rows, row)
	}

	if len(historyList.Rows) > 0 {
		slices.SortStableFunc(historyList.Rows, func(a, b HistoryListRow) int {
			if a.CrawledAt.Equal(b.CrawledAt) {
				return 0
			}
			if a.CrawledAt.After(b.CrawledAt) {
				return -1
			}
			return 1
		})

		historyList.FirstTime = &historyList.Rows[0].CrawledAt
		historyList.LastTime = &historyList.Rows[len(historyList.Rows)-1].CrawledAt
	}

	return &historyList, nil
}
