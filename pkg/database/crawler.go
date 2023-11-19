package database

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
)

type location struct {
	country   string
	city      string
	latitude  float64
	longitude float64
}

func (db *DB) IPToLocation(ip net.IP) (location, error) {
	if db.geoipDB == nil {
		return location{}, nil
	}

	ipRecord, err := db.geoipDB.City(ip)
	if err != nil {
		return location{}, fmt.Errorf("getting geoip failed: %w", err)
	}

	return location{
		country:   ipRecord.Country.Names["en"],
		city:      ipRecord.City.Names["en"],
		latitude:  ipRecord.Location.Latitude,
		longitude: ipRecord.Location.Longitude,
	}, nil
}

func randomHourSeconds() int64 {
	return rand.Int63n(3600)
}

func (db *DB) UpdateCrawledNodeFail(node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_fail", start, err)

	_, err = db.ExecRetryBusy(
		`
			INSERT INTO discovered_nodes (
				node_id,
				network_address,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?1,
				?2,
				?3,
				unixepoch(),
				unixepoch(),
				unixepoch() + ?6
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				last_found = unixepoch(),
				next_crawl = CASE
					WHEN ?4 == 'dial'
					THEN excluded.next_crawl
					ELSE next_crawl
				END;

			INSERT INTO crawl_history (
				node_id,
				crawled_at,
				direction,
				error
			) VALUES (
				?1,
				unixepoch(),
				?4,
				?5
			)
			ON CONFLICT (node_id, crawled_at) DO NOTHING;
		`,
		node.ID(),
		node.N.String(),
		node.N.IP().String(),
		node.Direction,
		node.Error,
		db.nextCrawlFail+randomHourSeconds(),
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) UpdateNotEthNode(node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_not_eth", start, err)

	_, err = db.ExecRetryBusy(
		`
			INSERT INTO discovered_nodes (
				node_id,
				network_address,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?1,
				?2,
				?3,
				unixepoch(),
				unixepoch(),
				unixepoch() + ?4
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				next_crawl = excluded.next_crawl
			WHERE ?5 == 'dial'
		`,
		node.ID(),
		node.N.String(),
		node.N.IP().String(),
		db.nextCrawlNotEth+randomHourSeconds(),
		node.Direction,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) UpdateCrawledNodeSuccess(node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_success", start, err)

	info := node.GetInfo()
	ip := node.N.IP()

	location, err := db.IPToLocation(ip)
	if err != nil {
		return fmt.Errorf("geoip failed: %w", err)
	}

	if len(node.BlockHeaders) != 0 {
		err = db.InsertBlocks(node.BlockHeaders, node.Info.NetworkID)
		if err != nil {
			return fmt.Errorf("inserting blocks failed: %w", err)
		}
	}

	clientPtr := parseClientID(&node.Info.ClientName)
	if clientPtr == nil {
		log.Error("parsing client ID failed", "id", node.Info.ClientName)
	}

	client := clientPtr.Deref()

	_, err = db.ExecRetryBusy(
		`
			INSERT INTO crawled_nodes (
				node_id,
				updated_at,
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
				next_fork_id,
				head_hash,
				ip_address,
				connection_type,
				country,
				city,
				latitude,
				longitude
			) VALUES (
				?1,					-- node_id
				unixepoch(),		-- updated_at
				nullif(?2, ''),		-- client_identifier
				nullif(?3, ''),		-- client_name,
				nullif(?4, ''),		-- client_user_data,
				nullif(?5, ''),		-- client_version,
				nullif(?6, ''),		-- client_build,
				nullif(?7, ''),		-- client_os,
				nullif(?8, ''),		-- client_arch,
				nullif(?9, ''),		-- client_language,
				nullif(?10, 0),		-- rlpx_version
				nullif(?11, ''),	-- capabilities
				nullif(?12, 0),		-- network_id
				nullif(?13, 0),		-- fork_id
				nullif(?14, 0),		-- next_fork_id
				nullif(?15, X''),	-- head_hash
				nullif(?16, ''),	-- ip_address
				nullif(?17, ''),	-- connection_type
				nullif(?18, ''),	-- country
				nullif(?19, ''),	-- city
				nullif(?20, 0.0),	-- latitude
				nullif(?21, 0.0)	-- longitude
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				updated_at = unixepoch(),
				client_name = coalesce(excluded.client_name, client_name),
				rlpx_version = coalesce(excluded.rlpx_version, rlpx_version),
				capabilities = coalesce(excluded.capabilities, capabilities),
				network_id = coalesce(excluded.network_id, network_id),
				fork_id = coalesce(excluded.fork_id, fork_id),
				next_fork_id = excluded.next_fork_id,			-- Not coalesce because no next fork is valid
				head_hash = coalesce(excluded.head_hash, head_hash),
				ip_address = coalesce(excluded.ip_address, ip_address),
				connection_type = coalesce(excluded.connection_type, connection_type),
				country = coalesce(excluded.country, country),
				city = coalesce(excluded.city, city),
				latitude = coalesce(excluded.latitude, latitude),
				longitude = coalesce(excluded.longitude, longitude);

			INSERT INTO discovered_nodes (
				node_id,
				network_address,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?1,
				?22,
				?16,
				unixepoch(),
				unixepoch(),
				unixepoch() + ?24
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				last_found = unixepoch(),
				-- Only update next_crawl if we initiated the connection.
				-- Even if the peer initiated the the connection, we still
				-- want to try dialing because we want to see if the node is
				-- exposed.
				next_crawl = CASE
					WHEN ?23 == 'dial'
						THEN excluded.next_crawl
						ELSE next_crawl
					END;

			INSERT INTO crawl_history (
				node_id,
				crawled_at,
				direction,
				error
			) VALUES (
				?1,
				unixepoch(),
				?23,
				NULL
			)
			ON CONFLICT (node_id, crawled_at) DO NOTHING;
		`,
		node.ID(),
		info.ClientName,
		client.Name,
		client.UserData,
		client.Version,
		client.Build,
		client.OS,
		client.Arch,
		client.Language,
		info.RLPxVersion,
		node.CapsString(),
		info.NetworkID,
		BytesToUnit32(info.ForkID.Hash[:]),
		info.ForkID.Next,
		info.HeadHash[:],
		node.N.IP().String(),
		node.ConnectionType(),
		location.country,
		location.city,
		location.latitude,
		location.longitude,
		node.N.String(),
		node.Direction,
		db.nextCrawlSucces+randomHourSeconds(),
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) InsertBlocks(blocks []*types.Header, networkID uint64) error {
	// tx, err := db.db.Begin()
	// if err != nil {
	// 	return fmt.Errorf("starting tx failed: %w", err)
	// }
	// defer tx.Rollback()

	stmt, err := db.db.Prepare(`
		INSERT INTO blocks (
			block_hash,
			network_id,
			timestamp,
			block_number,
			parent_hash
		) VALUES (
			?,
			?,
			?,
			?,
			?
		)
		ON CONFLICT (block_hash, network_id)
		DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("preparing statement failed: %w", err)
	}
	defer stmt.Close()

	for _, block := range blocks {
		start := time.Now()
		_, err = retryBusy(func() (sql.Result, error) {
			return stmt.Exec(
				block.Hash().Bytes(),
				networkID,
				block.Time,
				block.Number.Uint64(),
				block.ParentHash.Bytes(),
			)
		})
		metrics.ObserveDBQuery("insert_block", start, err)

		if err != nil {
			log.Error("upsert block failed", "err", err)
		}
	}

	// err = tx.Commit()
	// if err != nil {
	// 	return fmt.Errorf("commit failed: %w", err)
	// }

	return nil
}

func (db *DB) UpsertCrawledNode(node common.NodeJSON) error {
	db.wLock.Lock()
	defer db.wLock.Unlock()

	if !node.EthNode {
		err := db.UpdateNotEthNode(node)
		if err != nil {
			return fmt.Errorf("update not eth node failed: %w", err)
		}

		return nil
	}

	if node.Error != "" {
		err := db.UpdateCrawledNodeFail(node)
		if err != nil {
			return fmt.Errorf("update failed crawl failed: %w", err)
		}

		return nil
	}

	return db.UpdateCrawledNodeSuccess(node)
}

var missingBlockCache = map[uint64][]ethcommon.Hash{}
var missingBlocksLock = sync.Mutex{}

func (db *DB) GetMissingBlock(networkID uint64) (*ethcommon.Hash, error) {
	var err error

	missingBlocksLock.Lock()
	defer missingBlocksLock.Unlock()

	blocks, ok := missingBlockCache[networkID]
	if ok && len(blocks) != 0 {
		block := blocks[0]
		missingBlockCache[networkID] = blocks[1:]

		return &block, nil
	}

	start := time.Now()
	defer metrics.ObserveDBQuery("get_missing_block", start, err)

	rows, err := db.db.Query(
		`
			SELECT
				crawled.head_hash
			FROM crawled_nodes AS crawled
			LEFT JOIN blocks ON (crawled.head_hash = blocks.block_hash)
			WHERE
				crawled.network_id = ?1
				AND blocks.block_hash IS NULL
			LIMIT 1000
		`,
		networkID,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	newBlocks := make([]ethcommon.Hash, 0, 1000)

	for rows.Next() {
		var hash ethcommon.Hash

		err = rows.Scan(&hash)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil
			}

			return nil, fmt.Errorf("scan failed: %w", err)
		}

		newBlocks = append(newBlocks, hash)
	}

	if len(newBlocks) == 0 {
		return nil, nil
	}

	block := newBlocks[0]
	missingBlockCache[networkID] = newBlocks[1:]

	return &block, nil
}
