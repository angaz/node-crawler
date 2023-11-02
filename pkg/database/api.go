package database

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"golang.org/x/exp/slices"
)

type NodeTableHistory struct {
	CrawledAt time.Time
	Direction string
	Error     string
}

type ForkID [4]byte

func Uint32ToForkID(i uint32) ForkID {
	fid := ForkID{}
	binary.BigEndian.PutUint32(fid[:], i)

	return fid
}

func BytesToUnit32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

type NodeTable struct {
	nodeID         []byte
	updatedAt      *time.Time
	Enode          *string
	ClientName     *string
	RlpxVersion    *int64
	Capabilities   *string
	networkID      *int64
	ForkID         *ForkID
	NextForkID     *uint64
	HeadHash       *[]byte
	HeadHashTime   *time.Time
	IP             *string
	ConnectionType *string
	Country        *string
	City           *string
	Latitude       *float64
	Longitude      *float64
	nextCrawl      *time.Time

	History []NodeTableHistory
}

func (n NodeTable) RLPXVersion() string {
	if n.RlpxVersion == nil {
		return ""
	}

	return strconv.FormatInt(*n.RlpxVersion, 10)
}

func StringOrEmpty(v *string) string {
	if v == nil {
		return ""
	}

	return *v
}

func (n NodeTable) NodeID() string {
	return hex.EncodeToString(n.nodeID)
}

func (n NodeTable) NetworkID() string {
	if n.networkID == nil {
		return ""
	}

	return fmt.Sprintf("%s (%d)", NetworkName(n.networkID), *n.networkID)
}

func (n NodeTable) YOffsetPercent() int {
	if n.Latitude == nil {
		return 0
	}

	return 100 - int((*n.Latitude+90)/180*100)
}

func (n NodeTable) XOffsetPercent() int {
	if n.Longitude == nil {
		return 0
	}

	return int((*n.Longitude + 180) / 360 * 100)
}

var DateFormat = "2006-01-02 15:04:05 MST"

func (n NodeTable) HeadHashLine() string {
	if n.HeadHashTime == nil {
		return hex.EncodeToString(*n.HeadHash)
	}

	return fmt.Sprintf(
		"%s (%s)",
		hex.EncodeToString(*n.HeadHash),
		n.HeadHashTime.UTC().Format(DateFormat),
	)
}

func isSynced(updatedAt *time.Time, headHash *time.Time) string {
	if updatedAt == nil || headHash == nil {
		return "Unknown"
	}

	// If head hash is within one minute of the crawl time,
	// we can consider the node in sync
	if updatedAt.Sub(*headHash).Abs() < time.Minute {
		return "Yes"
	}

	return "No"
}

func (n NodeTable) IsSynced() string {
	return isSynced(n.updatedAt, n.HeadHashTime)
}

func sinceUpdate(updatedAt *time.Time) string {
	if updatedAt == nil {
		return "Never"
	}

	since := time.Since(*updatedAt)
	if since < 0 {
		return "In " + (-since).Truncate(time.Second).String()
	}

	return since.Truncate(time.Second).String() + " ago"
}

func (n NodeTable) UpdatedAt() string {
	return fmt.Sprintf(
		"%s (%s)",
		sinceUpdate(n.updatedAt),
		n.updatedAt.UTC().Format(DateFormat),
	)
}

func (n NodeTable) NextCrawl() string {
	if n.nextCrawl == nil {
		return "Never"
	}

	return fmt.Sprintf("%s (%s)", sinceUpdate(n.nextCrawl), n.nextCrawl.UTC().Format(DateFormat))
}

func NetworkName(networkID *int64) string {
	if networkID == nil {
		return "Unknown"
	}

	switch *networkID {
	case params.MainnetChainConfig.ChainID.Int64():
		return "Mainnet"
	case params.HoleskyChainConfig.ChainID.Int64():
		return "Holesky"
	case params.SepoliaChainConfig.ChainID.Int64():
		return "Sepolia"
	case params.GoerliChainConfig.ChainID.Int64():
		return "Goerli"
	default:
		return "Unknown"
	}
}

func (n NodeTable) NetworkName() string {
	return NetworkName(n.networkID)
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
				crawled.updated_at,
				network_address,
				client_name,
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
				next_crawl
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

	var updatedAtInt, headHashTimeInt, nextCrawlInt *int64
	var forkIDInt *uint32

	err = row.Scan(
		&nodePage.nodeID,
		&updatedAtInt,
		&nodePage.Enode,
		&nodePage.ClientName,
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
	)
	if err != nil {
		return nil, fmt.Errorf("row scan failed: %w", err)
	}

	nodePage.updatedAt = int64PrtToTimePtr(updatedAtInt)
	nodePage.HeadHashTime = int64PrtToTimePtr(headHashTimeInt)
	nodePage.nextCrawl = int64PrtToTimePtr(nextCrawlInt)

	if forkIDInt != nil {
		fid := Uint32ToForkID(*forkIDInt)
		nodePage.ForkID = &fid
	}

	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				crawled_at,
				direction,
				coalesce(error, '')
			FROM crawl_history
			WHERE
				node_id = ?
			LIMIT 10
		`,
		nodeIDBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("history query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		history := NodeTableHistory{}
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

		nodePage.History = append(nodePage.History, history)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("history rows iteration failed: %w", err)
	}

	return nodePage, nil
}

func int64PrtToTimePtr(i *int64) *time.Time {
	if i == nil {
		return nil
	}

	u := time.Unix(*i, 0)
	return &u
}

type NodeListRow struct {
	nodeID            []byte
	UpdatedAt         *time.Time
	ClientName        *string
	Country           *string
	HeadHashTimestamp *time.Time
}

func (n NodeListRow) NodeID() string {
	return hex.EncodeToString(n.nodeID)
}

func (n NodeListRow) SinceUpdate() string {
	return sinceUpdate(n.UpdatedAt)
}

func (n NodeListRow) IsSynced() string {
	return isSynced(n.UpdatedAt, n.HeadHashTimestamp)
}

type NodeList struct {
	PageNumber    int
	PageSize      int
	Synced        int
	Offset        int
	Total         int
	NetworkFilter int64
	List          []NodeListRow

	Networks []int64
}

func (l NodeList) NPages() int {
	return int(math.Ceil(float64(l.Total) / float64(l.PageSize)))
}

func (db *DB) GetNodeList(ctx context.Context, pageNumber int, networkID int64, synced int) (*NodeList, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_node_list", start, err)

	pageSize := 10
	offset := (pageNumber - 1) * pageSize

	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				node_id,
				updated_at,
				client_name,
				country,
				blocks.timestamp,
				COUNT(*) OVER () AS total
			FROM crawled_nodes AS crawled
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE
				(
					crawled.network_id = ?1
					OR ?1 = -1
				)
				AND (
					?2 = -1  -- All
					OR (     -- Not synced
						(
							abs(crawled.updated_at - blocks.timestamp) >= 60
							OR blocks.timestamp IS NULL
						)
						AND ?2 = 0
					)
					OR (     -- Synced
						abs(crawled.updated_at - blocks.timestamp) < 60
						AND ?2 = 1
					)
				)
			ORDER BY node_id
			LIMIT ?3
			OFFSET ?4
		`,
		networkID,
		synced,
		pageSize,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	out := NodeList{
		PageSize:      pageSize,
		PageNumber:    pageNumber,
		Synced:        synced,
		Offset:        offset,
		Total:         0,
		List:          []NodeListRow{},
		Networks:      []int64{},
		NetworkFilter: networkID,
	}

	for rows.Next() {
		row := NodeListRow{}
		var updatedAtInt, headHashTimeInt *int64

		err = rows.Scan(
			&row.nodeID,
			&updatedAtInt,
			&row.ClientName,
			&row.Country,
			&headHashTimeInt,
			&out.Total,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}

		row.UpdatedAt = int64PrtToTimePtr(updatedAtInt)
		row.HeadHashTimestamp = int64PrtToTimePtr(headHashTimeInt)

		out.List = append(out.List, row)
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows failed: %w", err)
	}

	rows.Close()

	rows, err = db.db.QueryContext(
		ctx,
		`
			SELECT
				DISTINCT network_id
			FROM crawled_nodes
			WHERE
				network_id IS NOT NULL
			ORDER BY network_id
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("networks query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var networkID int64

		err = rows.Scan(&networkID)
		if err != nil {
			return nil, fmt.Errorf("networks scan failed: %w", err)
		}

		out.Networks = append(out.Networks, networkID)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("networks rows failed: %w", err)
	}

	rows.Close()

	return &out, nil
}

type Stats struct {
	Client    Client
	NetworkID *int64
	Country   *string
	Synced    string
}

type AllStats []Stats

type StatsFilterFn func(Stats) bool

type Count struct {
	Key   string
	Count int
}

func (s AllStats) CountClientName(filters ...StatsFilterFn) []Count {
	count := map[string]int{}

	for _, stat := range s {
		skip := false
		for _, filter := range filters {
			if !filter(stat) {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		v, ok := count[stat.Client.Name]
		if !ok {
			v = 0
		}

		v += 1
		count[stat.Client.Name] = v
	}

	out := make([]Count, 0, len(count))

	for key, value := range count {
		out = append(out, Count{
			Key:   key,
			Count: value,
		})
	}

	slices.SortFunc(out, func(a, b Count) int {
		if a.Count == b.Count {
			return strings.Compare(a.Key, b.Key)
		}

		return a.Count - b.Count
	})

	return out
}

type Client struct {
	Name     string
	Version  string
	OS       string
	Language string
}

func parseClientName(clientName *string) *Client {
	if clientName == nil {
		return nil
	}

	name := strings.ToLower(*clientName)

	if name == "" {
		return nil
	}

	if name == "server" {
		return nil
	}

	if strings.HasPrefix(name, "nimbus-eth1") {
		newClientName := make([]rune, 0, len(name))
		for _, c := range name {
			switch c {
			case '[', ']', ':', ',':
				// NOOP
			default:
				newClientName = append(newClientName, c)
			}
		}

		parts := strings.Split(string(newClientName), " ")

		if len(parts) != 7 {
			log.Error("nimbus-eth1 not valid", "client_name", name)
		}

		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       parts[2],
			Language: "nim",
		}
	}

	parts := strings.Split(strings.ToLower(name), "/")

	if parts[0] == "" {
		return nil
	}

	switch len(parts) {
	case 1:
		return &Client{
			Name: parts[0],
		}
	case 2:
		return &Client{
			Name:    parts[0],
			Version: parts[1],
		}
	case 3:
		lang := ""

		if parts[0] == "reth" {
			lang = "rust"
		} else if parts[0] == "geth" {
			lang = "go"
		} else {
			log.Error("not reth or geth", "client_name", name)
		}

		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       parts[2],
			Language: lang,
		}
	case 4:
		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       parts[2],
			Language: parts[3],
		}
	case 5:
		return &Client{
			// Name:     parts[0] + "/" + parts[1],
			Name:     parts[0],
			Version:  parts[2],
			OS:       parts[3],
			Language: parts[4],
		}
	case 6:
		if parts[0] == "q-client" {
			return &Client{
				Name:     parts[0],
				Version:  parts[1],
				OS:       parts[4],
				Language: parts[5],
			}
		}
	case 7:
		return &Client{
			Name:     parts[0],
			Version:  parts[4],
			OS:       parts[5],
			Language: parts[6],
		}
	}

	log.Error("could not parse client", "client_name", name)

	return nil
}

func (db *DB) GetStats(ctx context.Context) (AllStats, error) {
	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				client_name,
				crawled.network_id,
				country,
				updated_at,
				blocks.timestamp
			FROM crawled_nodes AS crawled
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
		`,
	)
	if err != nil {
		return AllStats{}, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	allStats := make([]Stats, 1024)

	for rows.Next() {
		stats := Stats{}
		var name *string
		var updatedAtInt, blockTimestampInt *int64

		err := rows.Scan(
			&name,
			&stats.NetworkID,
			&stats.Country,
			&updatedAtInt,
			&blockTimestampInt,
		)
		if err != nil {
			return AllStats{}, fmt.Errorf("scan failed: %w", err)
		}

		updatedAt := int64PrtToTimePtr(updatedAtInt)
		blockTimestamp := int64PrtToTimePtr(blockTimestampInt)

		client := parseClientName(name)
		if client != nil {
			stats.Synced = isSynced(updatedAt, blockTimestamp)
			stats.Client = *client
			allStats = append(allStats, stats)
		}
	}

	return AllStats(allStats), nil
}
