package database

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
)

func (d *DB) UpsertNode(node *enode.Node) error {
	d.wLock.Lock()
	defer d.wLock.Unlock()

	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("disc_upsert_node", start, err)

	_, err = d.ExecRetryBusy(
		`
			INSERT INTO discovered_nodes (
				node_id,
				node_pubkey,
				node_record,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?,
				?,
				?,
				?,
				unixepoch(),
				unixepoch(),
				unixepoch()
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				node_record = best_record(node_record, excluded.node_record),
				ip_address = excluded.ip_address,
				last_found = unixepoch()
			WHERE last_found < unixepoch('now', '-1 hour')  -- Only update once an hour
		`,
		node.ID().Bytes(),
		common.PubkeyBytes(node.Pubkey()),
		common.EncodeENR(node.Record()),
		node.IP().String(),
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (d *DB) SelectDiscoveredNodeSlice(limit int) ([]*enode.Node, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("select_disc_node_slice", start, err)

	rows, err := d.db.Query(
		`
			SELECT
				node_record
			FROM discovered_nodes
			WHERE
				next_crawl < unixepoch()
			ORDER BY next_crawl ASC
			LIMIT ?
		`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	out := make([]*enode.Node, 0, limit)
	for rows.Next() {
		var enrBytes []byte

		err = rows.Scan(&enrBytes)
		if err != nil {
			return nil, fmt.Errorf("scanning row failed: %w", err)
		}

		record, err := common.LoadENR(enrBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to load enr: %w", err)
		}

		node, err := common.RecordToEnode(record)
		if err != nil {
			return nil, fmt.Errorf("parsing enr failed: %w, %s", err, enrBytes)
		}

		out = append(out, node)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows iteration failed: %w", err)
	}

	return out, nil
}
