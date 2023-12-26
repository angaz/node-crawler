package database

import (
	"context"
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
	"github.com/jackc/pgx/v5"
)

type location struct {
	country          string
	countryGeoNameID uint
	city             string
	cityGeoNameID    uint
	latitude         float64
	longitude        float64
}

func (db *DB) IPToLocation(ip net.IP) (location, error) {
	if db.geoipDB == nil {
		//nolint:exhaustruct
		return location{}, nil
	}

	ipRecord, err := db.geoipDB.City(ip)
	if err != nil {
		return location{}, fmt.Errorf("getting geoip failed: %w", err)
	}

	return location{
		country:          ipRecord.Country.Names["en"],
		countryGeoNameID: ipRecord.Country.GeoNameID,
		city:             ipRecord.City.Names["en"],
		cityGeoNameID:    ipRecord.City.GeoNameID,
		latitude:         ipRecord.Location.Latitude,
		longitude:        ipRecord.Location.Longitude,
	}, nil
}

func randomHourSeconds() int64 {
	return rand.Int63n(3600)
}

func (db *DB) UpdateCrawledNodeFail(ctx context.Context, tx pgx.Tx, node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_fail", start, err)

	ip := node.N.IP()

	location, err := db.IPToLocation(ip)
	if err != nil {
		return fmt.Errorf("ip to location: %w", err)
	}

	err = db.upsertCountryCity(ctx, tx, location)
	if err != nil {
		return fmt.Errorf("upsert country city: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
			WITH disc_upsert AS (
				INSERT INTO disc.nodes (
					node_id,
					node_type,
					first_found,
					last_found,
					next_crawl,
					node_pubkey,
					node_record,
					ip_address,
					city_geoname_id
				)
				VALUES (
					@node_id,
					@node_type,
					now(),
					now(),
					now() + make_interval(secs => @next_crawl),
					@node_pubkey,
					@node_record,
					@ip_address,
					@city_geoname_id
				)
				ON CONFLICT (node_id) DO UPDATE
				SET
					next_crawl = excluded.next_crawl
				WHERE @direction == 'dial'
			)

			INSERT INTO crawler.history (
				node_id,
				crawled_at,
				direction,
				error
			)
			VALUES (
				@node_id,
				now(),
				@direction,
				@error
			)
			ON CONFLICT (node_id, crawled_at) DO NOTHING;
		`,
		pgx.NamedArgs{
			"node_id":     node.ID(),
			"node_pubkey": common.PubkeyBytes(node.N.Pubkey()),
			"node_type":   common.ENRNodeType(node.N.Record()).String(),
			"node_record": common.EncodeENR(node.N.Record()),
			"ip_address":  ip.String(),
			"direction":   node.Direction.String(),
			"error":       node.Error,
			"next_crawl":  db.nextCrawlFail + randomHourSeconds(),
		},
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func (db *DB) UpdateNotEthNode(ctx context.Context, tx pgx.Tx, node common.NodeJSON) error {
	var err error

	defer metrics.ObserveDBQuery("update_crawled_node_not_eth", time.Now(), err)

	ip := node.N.IP()

	location, err := db.IPToLocation(ip)
	if err != nil {
		return fmt.Errorf("ip to location: %w", err)
	}

	err = db.upsertCountryCity(ctx, tx, location)
	if err != nil {
		return fmt.Errorf("upsert country city: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
			INSERT INTO disc.nodes (
				node_id,
				node_type,
				first_found,
				last_found,
				next_crawl,
				node_pubkey,
				node_record,
				ip_address,
				city_geoname_id
			)
			VALUES (
				@node_id,
				@node_type,
				now(),
				now(),
				now() + make_interval(secs => @next_crawl),
				@node_pubkey,
				@node_record,
				@ip_address,
				@city_geoname_id
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				next_crawl = excluded.next_crawl
			WHERE @direction == 'dial'
		`,
		pgx.NamedArgs{
			"node_id":     node.ID(),
			"node_pubkey": common.PubkeyBytes(node.N.Pubkey()),
			"node_type":   common.ENRNodeType(node.N.Record()).String(),
			"node_record": common.EncodeENR(node.N.Record()),
			"ip_address":  ip.String(),
			"direction":   node.Direction.String(),
			"error":       node.Error,
			"next_crawl":  db.nextCrawlNotEth + randomHourSeconds(),
		},
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func (db *DB) UpdateCrawledNodeSuccess(ctx context.Context, tx pgx.Tx, node common.NodeJSON) error {
	var err error

	defer metrics.ObserveDBQuery("update_crawled_node_success", time.Now(), err)

	info := node.GetInfo()
	ip := node.N.IP()

	location, err := db.IPToLocation(ip)
	if err != nil {
		return fmt.Errorf("geolocation: %w", err)
	}

	if len(node.BlockHeaders) != 0 {
		err = db.InsertBlocks(ctx, tx, node.Info.NetworkID, node.BlockHeaders)
		if err != nil {
			return fmt.Errorf("inserting blocks: %w", err)
		}
	}

	clientPtr := common.ParseClientID(&node.Info.ClientIdentifier)
	if clientPtr == nil && node.Info.ClientIdentifier != "" {
		log.Error("parsing client ID failed", "id", node.Info.ClientIdentifier)
	}

	client := clientPtr.Deref()

	err = db.upsertCountryCity(ctx, tx, location)
	if err != nil {
		return fmt.Errorf("upsert country city: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
			WITH client_identifier AS (
				INSERT INTO client.identifiers (
					client_identifier
				)
				VALUES (
					nullif(@client_identifier, 'Unknown')
				)
				ON CONFLICT (client_identifier) DO NOTHING
				RETURNING client_identifier_id
			), client_name AS (
				INSERT INTO client.names (
					client_name
				)
				VALUES (
					nullif(@client_name, 'Unknown')
				)
				ON CONFLICT (client_name) DO NOTHING
				RETURNING client_name_id
			), client_user_data AS (
				INSERT INTO client.user_data (
					client_user_data
				)
				VALUES (
					nullif(@client_user_data, 'Unknown')
				)
				ON CONFLICT (client_user_data) DO NOTHING
				RETURNING client_user_data_id
			), client_version AS (
				INSERT INTO client.versions (
					client_version
				)
				VALUES (
					nullif(@client_version, 'Unknown')
				)
				ON CONFLICT (client_version) DO NOTHING
				RETURNING client_version_id
			), client_build AS (
				INSERT INTO client.builds (
					client_build
				)
				VALUES (
					nullif(@client_build, 'Unknown')
				)
				ON CONFLICT (client_build) DO NOTHING
				RETURNING client_build_id
			), client_language AS (
				INSERT INTO client.languages (
					client_language
				)
				VALUES (
					nullif(@client_language, 'Unknown')
				)
				ON CONFLICT (client_language) DO NOTHING
				RETURNING client_language_id
			), capabilities AS (
				INSERT INTO execution.capabilities (
					capabilities
				)
				VALUES (
					nullif(@capabilities, 'Unknown')
				)
				ON CONFLICT (capabilities) DO NOTHING
				RETURNING capabilities_id
			), disc_node AS (
				INSERT INTO disc.nodes (
					node_id,
					node_type,
					first_found,
					last_found,
					next_crawl,
					node_pubkey,
					node_record,
					ip_address,
					city_geoname_id
				)
				VALUES (
					@node_id,
					@node_type,
					now(),
					now(),
					now() + make_interval(secs => @next_crawl),
					@node_pubkey,
					@node_record,
					@ip_address,
					@city_geoname_id
				)
				ON CONFLICT (node_id) DO UPDATE
				SET
					last_found = unixepoch(),
					-- Only update next_crawl if we initiated the connection.
					-- Even if the peer initiated the the connection, we still
					-- want to try dialing because we want to see if the node has
					-- good inbound network configuration.
					next_crawl = CASE
						WHEN @direction == 'dial'
							THEN excluded.next_crawl
							ELSE next_crawl
						END
			), crawled_node AS (
				INSERT INTO execution.nodes (
					node_id,
					updated_at,
					client_identifier_id,
					rlpx_version,
					capabilities_id,
					network_id,
					fork_id,
					next_fork_id,
					head_hash,
					client_name_id,
					client_user_data_id,
					client_version_id,
					client_build_id,
					client_os,
					client_arch,
					client_language_id
				) VALUES (
					@node_id,
					now(),
					(SELECT client_identifier_id FROM client_identifier),
					@rlpx_version,
					(SELECT capabilities_id FROM capabilities),
					@network_id,
					@fork_id,
					@next_fork_id,
					@head_hash,
					(SELECT client_name_id FROM client_name),
					(SELECT client_user_data_id FROM client_user_data),
					(SELECT client_version_id FROM client_version),
					(SELECT client_build_id FROM client_build),
					@client_os,
					@client_arch,
					(SELECT client_language_id FROM client_language)
				)
				ON CONFLICT (node_id) DO UPDATE
				SET
					updated_at = now(),
					client_identifier_id = excluded.client_identifier_id,
					rlpx_version = excluded.rlpx_version,
					capabilities_id = excluded.capabilities_id,
					network_id = excluded.network_id,
					fork_id = excluded.fork_id,
					next_fork_id = excluded.next_fork_id,
					head_hash = excluded.head_hash,
					client_name_id = excluded.client_name_id,
					client_user_data_id = excluded.client_user_data_id,
					client_version_id = excluded.client_version_id,
					client_build_id = excluded.client_build_id,
					client_os = excluded.client_os,
					client_arch = excluded.client_arch,
					client_language_id = excluded.client_language_id
			)

			INSERT INTO crawler.history (
				node_id,
				crawled_at,
				direction,
				error
			) VALUES (
				@node_id,
				now(),
				@direction,
				NULL
			)
			ON CONFLICT (node_id, crawled_at) DO NOTHING
		`,
		pgx.NamedArgs{
			"node_id":           node.ID(),
			"client_identifier": info.ClientIdentifier,
			"client_name":       client.Name,
			"client_user_data":  client.UserData,
			"client_version":    client.Version,
			"client_build":      client.Build,
			"client_os":         client.OS,
			"client_arch":       client.Arch,
			"client_language":   client.Language,
			"rlpx_version":      info.RLPxVersion,
			"capabilities":      node.CapsString(),
			"network_id":        info.NetworkID,
			"fork_id":           BytesToUnit32(info.ForkID.Hash[:]),
			"next_fork_id":      info.ForkID.Next,
			"head_hash":         info.HeadHash[:],
			"ip_address":        node.N.IP().String(),
			"city_geoname_id":   location.cityGeoNameID,
			"node_pubkey":       common.PubkeyBytes(node.N.Pubkey()),
			"node_type":         common.ENRNodeType(node.N.Record()),
			"node_record":       common.EncodeENR(node.N.Record()),
			"direction":         node.Direction,
			"next_crawl":        db.nextCrawlSucces + randomHourSeconds(),
		},
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) InsertBlocks(
	ctx context.Context,
	tx pgx.Tx,
	networkID uint64,
	blocks []*types.Header,
) error {
	stmt, err := tx.Prepare(
		ctx,
		"insert_blocks",
		`
			INSERT INTO execution.blocks (
				block_hash,
				network_id,
				timestamp,
				block_number
			) VALUES (
				$1,
				$2,
				$3,
				$4
			)
			ON CONFLICT (block_hash, network_id) DO NOTHING
		`,
	)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}

	for _, block := range blocks {
		start := time.Now()

		_, err = tx.Exec(
			ctx,
			stmt.Name,

			block.Hash().Bytes(),
			networkID,
			block.Number.Uint64(),
			block.Time,
		)

		metrics.ObserveDBQuery("insert_block", start, err)

		if err != nil {
			return fmt.Errorf("upsert: %w", err)
		}
	}

	return nil
}

func (db *DB) UpsertCrawledNode(ctx context.Context, tx pgx.Tx, node common.NodeJSON) error {
	defer metrics.NodeUpdateInc(node.Direction.String(), node.Error)

	if !node.EthNode {
		err := db.UpdateNotEthNode(ctx, tx, node)
		if err != nil {
			return fmt.Errorf("upsert not eth node: %w", err)
		}

		return nil
	}

	if node.Error != "" {
		err := db.UpdateCrawledNodeFail(ctx, tx, node)
		if err != nil {
			return fmt.Errorf("upsert failed crawl: %w", err)
		}

		return nil
	}

	err := db.UpdateCrawledNodeSuccess(ctx, tx, node)
	if err != nil {
		return fmt.Errorf("upsert success: %w", err)
	}

	return nil
}

var missingBlockCache = map[uint64][]ethcommon.Hash{}
var missingBlocksLock = sync.Mutex{}

func (db *DB) GetMissingBlock(ctx context.Context, tx pgx.Tx, networkID uint64) (*ethcommon.Hash, error) {
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

	// TODO: Optimize this. We have all the blocks at this time.
	return nil, nil

	// rows, err := db.QueryRetryBusy(
	// 	`
	// 		SELECT
	// 			crawled.head_hash
	// 		FROM crawled_nodes AS crawled
	// 		LEFT JOIN blocks ON (crawled.head_hash = blocks.block_hash)
	// 		WHERE
	// 			crawled.network_id = ?1
	// 			AND blocks.block_hash IS NULL
	// 		LIMIT 1000
	// 	`,
	// 	networkID,
	// )
	// if err != nil {
	// 	return nil, fmt.Errorf("query failed: %w", err)
	// }

	// newBlocks := make([]ethcommon.Hash, 0, 1000)

	// for rows.Next() {
	// 	var hash ethcommon.Hash

	// 	err = rows.Scan(&hash)
	// 	if err != nil {
	// 		if errors.Is(err, sql.ErrNoRows) {
	// 			return nil, nil
	// 		}

	// 		return nil, fmt.Errorf("scan failed: %w", err)
	// 	}

	// 	newBlocks = append(newBlocks, hash)
	// }

	// if len(newBlocks) == 0 {
	// 	return nil, nil
	// }

	// block := newBlocks[0]
	// missingBlockCache[networkID] = newBlocks[1:]

	// return &block, nil
}
