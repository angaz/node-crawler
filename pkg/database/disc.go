package database

import (
	"fmt"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
)

func (d *DB) UpsertNode(node *enode.Node) error {
	_, err := d.ExecRetryBusy(
		0,
		`
			INSERT INTO discovered_nodes (
				id,
				node,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?,
				?,
				?,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP
			)
			ON CONFLICT (id) DO UPDATE
			SET
				node = excluded.node,
				ip_address = excluded.ip_address,
				last_found = CURRENT_TIMESTAMP
			WHERE
				node != excluded.node
		`,
		node.ID().String(),
		node.String(),
		node.IP().String(),
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (d *DB) SelectDiscoveredNodeSlice(
	nIDStart string,
	nIDEnd string,
	limit int,
) ([]*enode.Node, error) {
	rows, err := d.db.Query(
		`
			SELECT
				node
			FROM discovered_nodes
			WHERE
				id >= ?
				AND id <= ?
				AND next_crawl < CURRENT_TIMESTAMP
			ORDER BY next_crawl ASC
			LIMIT ?
		`,
		nIDStart,
		nIDEnd,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	out := make([]*enode.Node, 0, limit)
	for rows.Next() {
		enr := ""

		err = rows.Scan(&enr)
		if err != nil {
			return nil, fmt.Errorf("scanning row failed: %w", err)
		}

		node, err := common.ParseNode(enr)
		if err != nil {
			return nil, fmt.Errorf("parsing enr failed: %w, %s", err, enr)
		}

		out = append(out, node)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows iteration failed: %w", err)
	}

	return out, nil
}