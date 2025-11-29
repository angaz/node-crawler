package database

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"log/slog"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/jackc/pgx/v5"
)

func randomHourSeconds() time.Duration {
	return time.Duration(rand.Int63n(3600)) * time.Second
}

func (db *DB) UpdateCrawledNodeFail(ctx context.Context, tx pgx.Tx, node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_fail", start, err)

	_, err = tx.Exec(
		ctx,
		`
			WITH disc_upsert AS (
				INSERT INTO disc.nodes (
					node_id,
					node_type,
					first_found,
					node_pubkey,
					node_record,
					ip_address
				)
				VALUES (
					@node_id,
					@node_type,
					now(),
					@node_pubkey,
					@node_record,
					@ip_address
				)
				ON CONFLICT (node_id) DO NOTHING
			), next_disc_crawl AS (
				INSERT INTO crawler.next_disc_crawl (
					node_id,
					last_found,
					next_crawl
				) VALUES (
					@node_id,
					now(),
					now()
				) ON CONFLICT (node_id) DO NOTHING
			), next_node_crawl AS (
				INSERT INTO crawler.next_node_crawl (
					node_id,
					updated_at,
					next_crawl,
					node_type
				) VALUES (
					@node_id,
					NULL,
					@next_crawl,
					@node_type
				)
				ON CONFLICT (node_id) DO UPDATE
				SET
					next_crawl = @next_crawl
				WHERE
					@direction = 'dial'::crawler.direction
			)

			INSERT INTO crawler.history
				SELECT
					@node_id node_id,
					now() crawled_at,
					@direction direction,
					@error error
				WHERE @direction = 'dial'
			ON CONFLICT (node_id, crawled_at) DO NOTHING
		`,
		pgx.NamedArgs{
			"node_id":     node.ID(),
			"node_pubkey": common.PubkeyBytes(node.N.Pubkey()),
			"node_type":   common.ENRNodeType(node.N.Record()).String(),
			"node_record": common.EncodeENR(node.N.Record()),
			"ip_address":  node.N.IP().String(),
			"direction":   node.Direction.String(),
			"error":       node.Error,
			"next_crawl":  time.Now().Add(db.nextCrawlFail + randomHourSeconds()),
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

	_, err = tx.Exec(
		ctx,
		`
			WITH disc_nodes AS (
				INSERT INTO disc.nodes (
					node_id,
					node_type,
					first_found,
					node_pubkey,
					node_record,
					ip_address
				)
				VALUES (
					@node_id,
					@node_type,
					now(),
					@node_pubkey,
					@node_record,
					@ip_address
				)
				ON CONFLICT (node_id) DO NOTHING
			), next_disc_crawl AS (
				INSERT INTO crawler.next_disc_crawl (
					node_id,
					last_found,
					next_crawl
				) VALUES (
					@node_id,
					now(),
					now()
				) ON CONFLICT (node_id) DO NOTHING
			)

			INSERT INTO crawler.next_node_crawl (
				node_id,
				updated_at,
				next_crawl,
				node_type
			) VALUES (
				@node_id,
				NULL,
				@next_crawl,
				@node_type
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				next_crawl = @next_crawl
			WHERE
				@direction = 'dial'::crawler.direction
		`,
		pgx.NamedArgs{
			"node_id":     node.ID(),
			"node_pubkey": common.PubkeyBytes(node.N.Pubkey()),
			"node_type":   common.ENRNodeType(node.N.Record()).String(),
			"node_record": common.EncodeENR(node.N.Record()),
			"ip_address":  node.N.IP().String(),
			"direction":   node.Direction.String(),
			"next_crawl":  time.Now().Add(db.nextCrawlNotEth + randomHourSeconds()),
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

	if len(node.BlockHeaders) != 0 {
		err = db.InsertBlocks(ctx, tx, node.Info.NetworkID, node.BlockHeaders)
		if err != nil {
			return fmt.Errorf("inserting blocks: %w", err)
		}
	}

	clientPtr := common.ParseClientID(&node.Info.ClientIdentifier)
	if clientPtr == nil && node.Info.ClientIdentifier != "" {
		slog.Error("parsing client ID failed", "id", node.Info.ClientIdentifier)
	}

	client := clientPtr.Deref()

	nextForkID := info.ForkID.Next

	// Some clients have a value that is in microseconds, convert to seconds
	if nextForkID > 9223372036854775807 {
		nextForkID /= 1e9
	}

	_, err = tx.Exec(
		ctx,
		`
			WITH client_ids AS (
				SELECT * FROM client.upsert(
					client_identifier	=> nullif(@client_identifier, 'Unknown'),
					client_name			=> nullif(@client_name, 'Unknown'),
					client_user_data	=> nullif(@client_user_data, 'Unknown'),
					client_version		=> nullif(@client_version, 'Unknown'),
					client_build		=> nullif(@client_build, 'Unknown'),
					client_os			=> @client_os::client.os,
					client_arch			=> @client_arch::client.arch,
					client_language		=> nullif(@client_language, 'Unknown')
				)
			), disc_node AS (
				INSERT INTO disc.nodes (
					node_id,
					node_type,
					first_found,
					node_pubkey,
					node_record,
					ip_address
				)
				VALUES (
					@node_id,
					@node_type,
					now(),
					@node_pubkey,
					@node_record,
					@ip_address
				)
				ON CONFLICT (node_id) DO NOTHING
			), next_disc_crawl AS (
				INSERT INTO crawler.next_disc_crawl (
					node_id,
					last_found,
					next_crawl
				) VALUES (
					@node_id,
					now(),
					now()
				) ON CONFLICT (node_id) DO NOTHING
			), next_node_crawl AS (
				INSERT INTO crawler.next_node_crawl (
					node_id,
					updated_at,
					next_crawl,
					node_type
				) VALUES (
					@node_id,
					now(),
					@next_crawl,
					@node_type
				)
				ON CONFLICT (node_id) DO UPDATE
				SET
					updated_at = excluded.updated_at,
					-- Only update next_crawl if we initiated the connection.
					-- Even if the peer initiated the the connection, we still
					-- want to try dialing because we want to see if the node has
					-- good inbound network configuration.
					next_crawl = CASE
						WHEN @direction = 'dial'::crawler.direction
							THEN excluded.next_crawl
							ELSE next_node_crawl.next_crawl
						END
			), crawled_node AS (
				INSERT INTO execution.nodes (
					node_id,
					client_identifier_id,
					rlpx_version,
					capabilities_id,
					network_id,
					fork_id,
					next_fork_id,
					head_hash
				) VALUES (
					@node_id,
					(SELECT client_identifier_id FROM client_ids),
					@rlpx_version,
					execution.upsert_capabilities(@capabilities),
					@network_id,
					@fork_id,
					@next_fork_id,
					@head_hash
				)
				ON CONFLICT (node_id) DO UPDATE
				SET
					client_identifier_id = excluded.client_identifier_id,
					rlpx_version = excluded.rlpx_version,
					capabilities_id = excluded.capabilities_id,
					network_id = excluded.network_id,
					fork_id = excluded.fork_id,
					next_fork_id = excluded.next_fork_id,
					head_hash = excluded.head_hash
				WHERE
					nodes.client_identifier_id != excluded.client_identifier_id
					OR nodes.rlpx_version != excluded.rlpx_version
					OR nodes.capabilities_id != excluded.capabilities_id
					OR nodes.network_id != excluded.network_id
					OR nodes.fork_id != excluded.fork_id
					OR nodes.next_fork_id != excluded.next_fork_id
					OR nodes.head_hash != excluded.head_hash
			)

			INSERT INTO crawler.history
				SELECT
					@node_id node_id,
					now() crawled_at,
					@direction direction,
					NULL error
				WHERE @direction = 'dial'
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
			"next_fork_id":      nextForkID,
			"head_hash":         info.HeadHash[:],
			"ip_address":        node.N.IP().String(),
			"node_pubkey":       common.PubkeyBytes(node.N.Pubkey()),
			"node_type":         common.ENRNodeType(node.N.Record()),
			"node_record":       common.EncodeENR(node.N.Record()),
			"direction":         node.Direction,
			"next_crawl":        time.Now().Add(db.nextCrawlSucces + randomHourSeconds()),
		},
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w, %#v, %#v %#v", err, node, client, info)
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
				to_timestamp($3),
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
			block.Time,
			block.Number.Uint64(),
		)

		metrics.ObserveDBQuery("insert_block", start, err)

		if err != nil {
			return fmt.Errorf("upsert: %w", err)
		}
	}

	return nil
}

type UpdateLimiter struct {
	m    map[string]time.Time
	ttl  time.Duration
	lock sync.Mutex
}

func NewUpdateLimiter(ttl time.Duration) *UpdateLimiter {
	limiter := &UpdateLimiter{
		m:    map[string]time.Time{},
		ttl:  ttl,
		lock: sync.Mutex{},
	}

	go limiter.runCleaner()

	return limiter
}

func (l *UpdateLimiter) runCleaner() {
	for {
		time.Sleep(time.Minute)

		l.lock.Lock()

		for key, ts := range l.m {
			if time.Since(ts) > l.ttl {
				delete(l.m, key)
			}
		}

		l.lock.Unlock()
	}
}

func (l *UpdateLimiter) IsLimited(node common.NodeJSON) bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	id := node.IDString()

	t, found := l.m[id]
	if found && time.Since(t) < l.ttl {
		return true
	}

	l.m[id] = time.Now()

	return false
}

var limiter = NewUpdateLimiter(3 * time.Hour)

func (db *DB) UpsertCrawledNode(ctx context.Context, tx pgx.Tx, node common.NodeJSON) error {
	defer metrics.NodeUpdateInc(node.Direction.String(), node.Error)

	// Sometimes the same node connects many times and we should just ignore it.
	if node.Error == "already connected" {
		return nil
	}

	if limiter.IsLimited(node) {
		return nil
	}

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

func (db *DB) setMissingBlocks(ctx context.Context) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_missing_block", start, err)

	rows, err := db.pg.Query(
		ctx,
		`
			SELECT DISTINCT
				network_id,
				head_hash
			FROM execution.nodes
			WHERE NOT EXISTS (
				SELECT 1
				FROM execution.blocks
				WHERE
					blocks.block_hash = nodes.head_hash
					AND blocks.network_id = nodes.network_id
			)
			ORDER BY
				network_id,
				head_hash
		`,
	)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	newCache := make(map[uint64][]ethcommon.Hash, 10)

	var networkID uint64
	var hash ethcommon.Hash

	_, err = pgx.ForEachRow(rows, []any{&networkID, &hash}, func() error {
		blocks, ok := newCache[networkID]
		if !ok {
			newCache[networkID] = []ethcommon.Hash{hash}
		} else {
			newCache[networkID] = append(blocks, hash)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}

	missingBlocksLock.Lock()
	defer missingBlocksLock.Unlock()

	missingBlockCache = newCache

	return nil
}

// Meant to be run as a goroutine.
//
// Updates the missing block cache.
func (db *DB) MissingBlocksDaemon(ctx context.Context, frequency time.Duration) {
	for ctx.Err() == nil {
		err := db.setMissingBlocks(ctx)
		if err != nil {
			slog.Error("missing blocks daemon update failed", "err", err)
		}

		next := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(next))
	}
}

func (db *DB) GetMissingBlock(ctx context.Context, tx pgx.Tx, networkID uint64) (*ethcommon.Hash, error) {
	missingBlocksLock.Lock()
	defer missingBlocksLock.Unlock()

	blocks, ok := missingBlockCache[networkID]
	if !ok || len(blocks) == 0 {
		return nil, nil
	}

	block := blocks[0]
	missingBlockCache[networkID] = blocks[1:]

	return &block, nil
}
