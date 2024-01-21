package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/jackc/pgx/v5"
)

func ptr[T any](value T) *T {
	return &value
}

func (db *DB) UpsertPortalDiscNode(ctx context.Context, tx pgx.Tx, node *enode.Node) error {
	if db.portalRecentlyCrawled.ContainsOrPush(node.ID()) {
		return nil
	}

	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("portal_disc_upsert_node", start, err)

	clientIdentifier := common.ENRClientName(node.Record())
	client := common.ParsePortalClientID(&clientIdentifier).Deref()

	_, err = tx.Exec(
		ctx,
		`
			WITH disc_node AS (
				INSERT INTO portal.disc_nodes (
					node_id,
					first_found,
					node_pubkey,
					node_record,
					ip_address,
					client_identifier_id
				) VALUES (
					@node_id,
					now(),
					@node_pubkey,
					@node_record,
					@ip_address,
					(SELECT client_identifier_id FROM client.upsert(
						client_identifier	=> nullif(@client_identifier, 'Unknown'),
						client_name			=> nullif(@client_name, 'Unknown'),
						client_user_data	=> NULL,
						client_version		=> nullif(@client_version, 'Unknown'),
						client_build		=> nullif(@client_build, 'Unknown'),
						client_os			=> 'Unknown',
						client_arch			=> 'Unknown',
						client_language		=> NULL
					))
				)
				ON CONFLICT (node_id) DO UPDATE
				SET
					node_id = excluded.node_id,
					node_pubkey = excluded.node_pubkey,
					node_record = excluded.node_record,
					ip_address = excluded.ip_address,
					client_identifier_id = excluded.client_identifier_id
				WHERE disc_nodes.node_record != excluded.node_record
			), next_disc_node AS (
				INSERT INTO portal.next_disc_crawl (
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

			SELECT 1
		`,
		pgx.NamedArgs{
			"node_id":           node.ID(),
			"node_pubkey":       common.PubkeyBytes(node.Pubkey()),
			"node_record":       common.EncodeENR(node.Record()),
			"ip_address":        node.IP().String(),
			"next_crawl":        time.Now().Add(12 * time.Hour).Add(randomHourSeconds()),
			"client_identifier": clientIdentifier,
			"client_name":       client.Name,
			"client_version":    client.Version,
			"client_build":      client.Build,
		},
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func (db *DB) UpdatePortalDiscNodeFailed(ctx context.Context, tx pgx.Tx, nodeID enode.ID) error {
	var err error

	defer metrics.ObserveDBQuery("portal_disc_update_node_failed", time.Now(), err)

	_, err = tx.Exec(
		ctx,
		`
			UPDATE portal.next_disc_crawl
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

func (db *DB) fetchPortalDiscToCrawl(ctx context.Context) error {
	var err error

	defer metrics.ObserveDBQuery("select_disc_nodes", time.Now(), err)

	rows, err := db.pg.Query(
		ctx,
		`
			SELECT
				next_crawl,
				node_record
			FROM portal.next_disc_crawl
			LEFT JOIN portal.disc_nodes USING (node_id)
			ORDER BY next_crawl
			LIMIT 1024
		`,
	)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	err = scanNodesToCrawl(rows, db.portalDiscToCrawlCache)
	if err != nil {
		return fmt.Errorf("scan nodes: %w", err)
	}

	return nil
}

func (db *DB) PortalDiscCrawlDone(nodeID enode.ID) {
	db.portalDiscToCrawlLock.Lock()
	defer db.portalDiscToCrawlLock.Unlock()

	delete(db.portalDiscActiveCrawlers, nodeID)
}

func (db *DB) PortalDiscToCrawl(ctx context.Context) (enode.ID, error) {
	db.portalDiscToCrawlLock.Lock()
	defer db.portalDiscToCrawlLock.Unlock()

	for ctx.Err() == nil {
		select {
		case nextNode := <-db.portalDiscToCrawlCache:
			if nextNode == nil {
				continue
			}

			nodeID := nextNode.Enode.ID()

			_, ok := db.portalDiscActiveCrawlers[nodeID]
			if ok {
				continue
			}

			db.portalDiscActiveCrawlers[nodeID] = struct{}{}

			return nodeID, nil
		default:
			err := db.fetchPortalDiscToCrawl(ctx)
			if err != nil {
				slog.Error("fetch nodes to crawl failed", "err", err)
				time.Sleep(time.Minute)
			}
		}
	}

	return enode.ID{}, ctx.Err()
}
