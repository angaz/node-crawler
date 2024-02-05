package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"log/slog"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/fifomemory"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type execer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (_ *DB) selectBestRecord(ctx context.Context, db rowQuerier, node *enode.Node) (*enr.Record, error) {
	var savedRecordBytes []byte
	var savedRecord *enr.Record

	row := db.QueryRow(
		ctx,
		`
			SELECT
				node_record
			FROM disc.nodes
			WHERE node_id = @node_id
		`,
		pgx.NamedArgs{
			"node_id": node.ID(),
		},
	)

	err := row.Scan(&savedRecordBytes)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("query node_record: %w", err)
	}

	if savedRecordBytes != nil {
		savedRecord, err = common.LoadENR(savedRecordBytes)
		if err != nil {
			return nil, fmt.Errorf("load saved record: %w", err)
		}
	}

	bestRecord := common.BestRecord(savedRecord, node.Record())

	return bestRecord, nil
}

func (db *DB) UpdateDiscNodeFailed(ctx context.Context, tx pgx.Tx, nodeID enode.ID) error {
	var err error

	defer metrics.ObserveDBQuery("disc_update_node_failed", time.Now(), err)

	_, err = tx.Exec(
		ctx,
		`
			UPDATE crawler.next_disc_crawl
			SET
				next_crawl = @next_crawl
			WHERE
				node_id = @node_id
		`,
		pgx.NamedArgs{
			"node_id":    nodeID.Bytes(),
			"next_crawl": time.Now().Add(36 * time.Hour).Add(randomHourSeconds()),
		},
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func (db *DB) UpsertNode(ctx context.Context, tx pgx.Tx, node *enode.Node) error {
	if db.discUpdateCache.ContainsOrPush(node.ID()) {
		return nil
	}

	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("disc_upsert_node", start, err)

	bestRecord, err := db.selectBestRecord(ctx, tx, node)
	if err != nil {
		return fmt.Errorf("select best record: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
			WITH disc_node AS (
				INSERT INTO disc.nodes (
					node_id,
					node_type,
					first_found,
					node_pubkey,
					node_record,
					ip_address
				) VALUES (
					@node_id,
					@node_type,
					now(),
					@node_pubkey,
					@node_record,
					@ip_address
				)
				ON CONFLICT (node_id) DO UPDATE
				SET
					node_type = excluded.node_type,
					node_record = excluded.node_record,
					ip_address = excluded.ip_address
				WHERE
					nodes.node_record != excluded.node_record
			), next_disc_node AS (
				INSERT INTO crawler.next_disc_crawl (
					node_id,
					last_found,
					next_crawl
				) VALUES (
					@node_id,
					now(),
					now()
				) ON CONFLICT (node_id) DO UPDATE
				SET
					last_found = now(),
					next_crawl = @next_crawl
			)

			INSERT INTO crawler.next_node_crawl (
				node_id,
				updated_at,
				next_crawl,
				node_type
			) VALUES (
				@node_id,
				NULL,
				now(),
				@node_type
			) ON CONFLICT (node_id) DO NOTHING

		`,
		pgx.NamedArgs{
			"node_id":     node.ID().Bytes(),
			"node_type":   common.ENRNodeType(node.Record()),
			"node_pubkey": common.PubkeyBytes(node.Pubkey()),
			"node_record": common.EncodeENR(bestRecord),
			"ip_address":  node.IP().String(),
			"next_crawl":  time.Now().Add(12 * time.Hour).Add(randomHourSeconds()),
		},
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

type NodeToCrawl struct {
	NextCrawl time.Time
	Enode     *enode.Node
}

func scanNodesToCrawl(rows pgx.Rows, ch chan<- *NodeToCrawl) error {
	var nextCrawl time.Time
	var enrBytes []byte

	_, err := pgx.ForEachRow(rows, []any{&nextCrawl, &enrBytes}, func() error {
		record, err := common.LoadENR(enrBytes)
		if err != nil {
			return fmt.Errorf("load enr: %w", err)
		}

		node, err := common.RecordToEnode(record)
		if err != nil {
			return fmt.Errorf("record to enode: %w, %x", err, enrBytes)
		}

		ch <- &NodeToCrawl{
			NextCrawl: nextCrawl,
			Enode:     node,
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("collect rows: %w", err)
	}

	return nil
}

func (db *DB) fetchExecutionNodesToCrawl(ctx context.Context) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("select_execution_node", start, err)

	rows, err := db.pg.Query(
		ctx,
		`
			SELECT
				next_node_crawl.next_crawl,
				node_record
			FROM crawler.next_node_crawl
			LEFT JOIN disc.nodes USING (node_id)
			LEFT JOIN crawler.next_disc_crawl USING (node_id)
			WHERE
				next_node_crawl.node_type IN ('Unknown', 'Execution')
				AND last_found > now() - INTERVAL '48 hours'
			ORDER BY next_node_crawl.next_crawl
			LIMIT 1024
		`,
	)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	err = scanNodesToCrawl(rows, db.executionNodesToCrawlCache)
	if err != nil {
		return fmt.Errorf("scan nodes: %w", err)
	}

	return nil
}

func (db *DB) fetchDiscNodesToCrawl(ctx context.Context) error {
	var err error

	defer metrics.ObserveDBQuery("select_disc_nodes", time.Now(), err)

	rows, err := db.pg.Query(
		ctx,
		`
			SELECT
				next_crawl,
				node_record
			FROM crawler.next_disc_crawl
			LEFT JOIN disc.nodes USING (node_id)
			ORDER BY next_crawl
			LIMIT 1024
		`,
	)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	err = scanNodesToCrawl(rows, db.discNodesToCrawlCache)
	if err != nil {
		return fmt.Errorf("scan nodes: %w", err)
	}

	return nil
}

func (db *DB) fetchConsensusNodesToCrawl(ctx context.Context) error {
	var err error

	defer metrics.ObserveDBQuery("select_consensus_nodes", time.Now(), err)

	rows, err := db.pg.Query(
		ctx,
		`
			SELECT
				next_crawl,
				node_record
			FROM crawler.next_node_crawl
			LEFT JOIN disc.nodes USING (node_id)
			WHERE
				next_node_crawl.node_type = 'Consensus'
			ORDER BY next_crawl
			LIMIT 1024
		`,
	)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	err = scanNodesToCrawl(rows, db.consensusNodesToCrawlCache)
	if err != nil {
		return fmt.Errorf("scan nodes: %w", err)
	}

	return nil
}

func nodesToCrawl(
	ctx context.Context,
	sleep bool,
	lock *sync.Mutex,
	ch <-chan *NodeToCrawl,
	recentlyCrawled *fifomemory.FIFOMemory[enode.ID],
	fetchNodesToCrawl func(context.Context) error,
) (*enode.Node, error) {
	lock.Lock()
	defer lock.Unlock()

	for ctx.Err() == nil {
		select {
		case nextNode := <-ch:
			if nextNode == nil {
				continue
			}

			if recentlyCrawled.ContainsOrPush(nextNode.Enode.ID()) {
				continue
			}

			if sleep {
				sleepDuration := time.Until(nextNode.NextCrawl)

				if sleepDuration > 15*time.Minute {
					continue
				}

				time.Sleep(sleepDuration)
			}

			return nextNode.Enode, nil
		default:
			err := fetchNodesToCrawl(ctx)
			if err != nil {
				slog.Error("fetch nodes to crawl failed", "err", err)
				time.Sleep(time.Minute)
			}
		}
	}

	return nil, ctx.Err()
}

func (db *DB) ConsensusNodesToCrawl(ctx context.Context) (*enode.Node, error) {
	return nodesToCrawl(
		ctx,
		true,
		db.consensusNodesToCrawlLock,
		db.consensusNodesToCrawlCache,
		db.consensusRecentlyCrawled,
		db.fetchConsensusNodesToCrawl,
	)
}

func (db *DB) ExecutionNodesToCrawl(ctx context.Context) (*enode.Node, error) {
	return nodesToCrawl(
		ctx,
		true,
		db.executionNodesToCrawlLock,
		db.executionNodesToCrawlCache,
		db.executionRecentlyCrawled,
		db.fetchExecutionNodesToCrawl,
	)
}

func (db *DB) DiscNodesToCrawl(ctx context.Context) (*enode.Node, error) {
	return nodesToCrawl(
		ctx,
		false,
		db.discNodesToCrawlLock,
		db.discNodesToCrawlCache,
		db.discRecentlyCrawled,
		db.fetchDiscNodesToCrawl,
	)
}
