package database

import (
	"database/sql"
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/common"
)

func (db *DB) MigrateCrawler() error {
	return db.migrate(
		"main",
		[]migrationFn{
			migrationCrawler000Schema,
			migrationCrawler001CrawledNodes,
			migrationCrawler002ENRBlob,
			migrationCrawler003NodePubkey,
			migrationCrawler004NodeType,
		},
		migrateCrawlerIndexes,
	)
}

func migrateCrawlerIndexes(tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE INDEX IF NOT EXISTS discovered_nodes_node_id_next_crawl
			ON discovered_nodes (node_id, next_crawl);
		CREATE INDEX IF NOT EXISTS discovered_nodes_next_crawl
			ON discovered_nodes (next_crawl);
		CREATE INDEX IF NOT EXISTS discovered_nodes_ip_address_node_id
			ON discovered_nodes (ip_address, node_id);

		CREATE INDEX IF NOT EXISTS crawled_nodes_node_id_last_seen
			ON crawled_nodes (node_id, updated_at);
		CREATE INDEX IF NOT EXISTS crawled_nodes_ip_address
			ON crawled_nodes (ip_address);
		CREATE INDEX IF NOT EXISTS crawled_nodes_client_name
			ON crawled_nodes (client_name);
		CREATE INDEX IF NOT EXISTS crawled_nodes_client_user_data
			ON crawled_nodes (client_user_data)
			WHERE client_user_data IS NOT NULL;
		CREATE INDEX IF NOT EXISTS crawled_nodes_network_id_head_hash
			ON crawled_nodes (network_id, head_hash);
		DROP INDEX IF EXISTS crawled_nodes_network_id;

		CREATE INDEX IF NOT EXISTS crawl_history_crawled_at
			ON crawl_history (crawled_at);

		CREATE INDEX IF NOT EXISTS blocks_block_hash_timestamp
			ON blocks (block_hash, timestamp);
	`)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func migrationCrawler000Schema(tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE discovered_nodes (
			node_id			BLOB	PRIMARY KEY,
			network_address	TEXT	NOT NULL,
			ip_address		TEXT	NOT NULL,
			first_found		INTEGER	NOT NULL,
			last_found		INTEGER	NOT NULL,
			next_crawl		INTEGER	NOT NULL
		) STRICT;

		CREATE TABLE crawled_nodes (
			node_id				BLOB	PRIMARY KEY,
			updated_at			INTEGER	NOT NULL,
			client_identifier	TEXT	DEFAULT NULL,
			client_name			TEXT	DEFAULT NULL,
			client_user_data	TEXT	DEFAULT NULL,
			client_version		TEXT	DEFAULT NULL,
			client_build		TEXT	DEFAULT NULL,
			client_os			TEXT	DEFAULT NULL,
			client_arch			TEXT	DEFAULT NULL,
			client_language		TEXT	DEFAULT NULL,
			rlpx_version		INTEGER	DEFAULT NULL,
			capabilities		TEXT	DEFAULT NULL,
			network_id			INTEGER	DEFAULT NULL,
			fork_id				INTEGER	DEFAULT NULL,
			next_fork_id		INTEGER	DEFAULT NULL,
			head_hash			BLOB	DEFAULT NULL,
			ip_address			TEXT	DEFAULT NULL,
			connection_type		TEXT	DEFAULT NULL,
			country				TEXT	DEFAULT NULL,
			city				TEXT	DEFAULT NULL,
			latitude			REAL	DEFAULT NULL,
			longitude			REAL	DEFAULT NULL
		) STRICT;

		CREATE TABLE crawl_history (
			node_id		BLOB	NOT NULL,
			crawled_at	INTEGER	NOT NULL,
			direction	TEXT	NOT NULL,
			error		TEXT	DEFAULT NULL,

			PRIMARY KEY (node_id, crawled_at)
		) STRICT;

		CREATE TABLE blocks (
			block_hash		BLOB	NOT NULL,
			network_id		INTEGER NOT NULL,
			timestamp		INTEGER	NOT NULL,
			block_number	INTEGER	NOT NULL,

			PRIMARY KEY (block_hash, network_id)
		) STRICT;
	`)
	if err != nil {
		return fmt.Errorf("create initial schema failed: %w", err)
	}

	return nil
}

func migrationCrawler001CrawledNodes(tx *sql.Tx) error {
	_, err := tx.Exec("ALTER TABLE crawled_nodes RENAME TO crawled_nodes_old")
	if err != nil {
		return fmt.Errorf("error renaming table: %w", err)
	}

	_, err = tx.Exec(`
		CREATE TABLE crawled_nodes (
			node_id				BLOB	PRIMARY KEY,
			updated_at			INTEGER	NOT NULL,
			client_identifier	TEXT	DEFAULT NULL,
			client_name			TEXT	DEFAULT NULL,
			client_user_data	TEXT	DEFAULT NULL,
			client_version		TEXT	DEFAULT NULL,
			client_build		TEXT	DEFAULT NULL,
			client_os			TEXT	DEFAULT NULL,
			client_arch			TEXT	DEFAULT NULL,
			client_language		TEXT	DEFAULT NULL,
			rlpx_version		INTEGER	DEFAULT NULL,
			capabilities		TEXT	DEFAULT NULL,
			network_id			INTEGER	DEFAULT NULL,
			fork_id				INTEGER	DEFAULT NULL,
			next_fork_id		INTEGER	DEFAULT NULL,
			head_hash			BLOB	DEFAULT NULL,
			ip_address			TEXT	DEFAULT NULL,
			connection_type		TEXT	DEFAULT NULL,
			country				TEXT	DEFAULT NULL,
			city				TEXT	DEFAULT NULL,
			latitude			REAL	DEFAULT NULL,
			longitude			REAL	DEFAULT NULL
		) STRICT;
	`)
	if err != nil {
		return fmt.Errorf("create new table failed: %w", err)
	}

	rows, err := tx.Query(`
		SELECT
			node_id,
			updated_at,
			client_identifier,
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
		FROM crawled_nodes_old
	`)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	stmt, err := tx.Prepare(`
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
			?,
			?,
			?,
			nullif(?, 'Unknown'),
			nullif(?, 'Unknown'),
			nullif(?, 'Unknown'),
			nullif(?, 'Unknown'),
			nullif(?, 'Unknown'),
			nullif(?, 'Unknown'),
			nullif(?, 'Unknown'),
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?
		)
	`)
	if err != nil {
		return fmt.Errorf("prepared statement failed: %w", err)
	}
	defer stmt.Close()

	for rows.Next() {
		var id, headHash []byte
		var forkID *uint32
		var clientID, caps, ipAddress, connectionType, country, city *string
		var updatedAt int64
		var rlpxVersion, networkID, nextForkID *int64
		var latitude, longitude *float64

		rows.Scan(
			&id,
			&updatedAt,
			&clientID,
			&rlpxVersion,
			&caps,
			&networkID,
			&forkID,
			&nextForkID,
			&headHash,
			&ipAddress,
			&connectionType,
			&country,
			&city,
			&latitude,
			&longitude,
		)

		clientPtr := parseClientID(clientID)
		if clientPtr == nil {
			if clientID != nil {
				log.Error("parsing client id failed", "id", *clientID)
			}
		}

		client := clientPtr.Deref()

		_, err = stmt.Exec(
			id,
			updatedAt,
			clientID,
			client.Name,
			client.UserData,
			client.Version,
			client.Build,
			client.OS,
			client.Arch,
			client.Language,
			rlpxVersion,
			caps,
			networkID,
			forkID,
			nextForkID,
			headHash,
			ipAddress,
			connectionType,
			country,
			city,
			latitude,
			longitude,
		)
		if err != nil {
			return fmt.Errorf("insert exec failed: %w", err)
		}
	}

	_, err = tx.Exec("DROP TABLE crawled_nodes_old")
	if err != nil {
		return fmt.Errorf("drop old table failed: %w", err)
	}

	return nil
}

func migrationCrawler002ENRBlob(tx *sql.Tx) error {
	_, err := tx.Exec("ALTER TABLE discovered_nodes RENAME TO discovered_nodes_old")
	if err != nil {
		return fmt.Errorf("renaming table failed: %w", err)
	}

	_, err = tx.Exec(`
		CREATE TABLE discovered_nodes (
			node_id		BLOB	PRIMARY KEY,
			node_record	BLOB	NOT NULL,
			ip_address	TEXT	NOT NULL,
			first_found	INTEGER	NOT NULL,
			last_found	INTEGER	NOT NULL,
			next_crawl	INTEGER	NOT NULL
		) STRICT;
	`)
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}

	oldRows, err := tx.Query(`
		SELECT
			node_id,
			network_address,
			ip_address,
			first_found,
			last_found,
			next_crawl
		FROM discovered_nodes_old
	`)
	if err != nil {
		return fmt.Errorf("querying old nodes failed: %w", err)
	}
	defer oldRows.Close()

	stmt, err := tx.Prepare(`
		INSERT INTO discovered_nodes (
			node_id,
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
			?,
			?
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare failed: %w", err)
	}
	defer stmt.Close()

	for oldRows.Next() {
		var nodeID []byte
		var networkAddress, ipAddress string
		var firstFound, lastFound, nextCrawl int64

		err := oldRows.Scan(
			&nodeID,
			&networkAddress,
			&ipAddress,
			&firstFound,
			&lastFound,
			&nextCrawl,
		)
		if err != nil {
			return fmt.Errorf("scan old row: %w", err)
		}

		enode, err := common.ParseNode(networkAddress)
		if err != nil {
			return fmt.Errorf("parse node: %s failed: %w", networkAddress, err)
		}

		_, err = stmt.Exec(
			nodeID,
			common.EncodeENR(enode.Record()),
			ipAddress,
			firstFound,
			lastFound,
			nextCrawl,
		)
		if err != nil {
			return fmt.Errorf("insert row failed: %w", err)
		}
	}

	_, err = tx.Exec("DROP TABLE discovered_nodes_old")
	if err != nil {
		return fmt.Errorf("drop old table failed: %w", err)
	}

	return nil
}

func migrationCrawler003NodePubkey(tx *sql.Tx) error {
	_, err := tx.Exec("ALTER TABLE discovered_nodes RENAME TO discovered_nodes_old")
	if err != nil {
		return fmt.Errorf("renaming table: %w", err)
	}

	_, err = tx.Exec(`
		CREATE TABLE discovered_nodes (
			node_id		BLOB	PRIMARY KEY,
			node_pubkey	BLOB	NOT NULL,
			node_record	BLOB	NOT NULL,
			ip_address	TEXT	NOT NULL,
			first_found	INTEGER	NOT NULL,
			last_found	INTEGER	NOT NULL,
			next_crawl	INTEGER	NOT NULL
		) STRICT;
	`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	oldRows, err := tx.Query(`
		SELECT
			node_id,
			node_record,
			ip_address,
			first_found,
			last_found,
			next_crawl
		FROM discovered_nodes_old
	`)
	if err != nil {
		return fmt.Errorf("querying old nodes: %w", err)
	}
	defer oldRows.Close()

	stmt, err := tx.Prepare(`
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
			?,
			?,
			?
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for oldRows.Next() {
		var nodeID, nodeRecord []byte
		var ipAddress string
		var firstFound, lastFound, nextCrawl int64

		err := oldRows.Scan(
			&nodeID,
			&nodeRecord,
			&ipAddress,
			&firstFound,
			&lastFound,
			&nextCrawl,
		)
		if err != nil {
			return fmt.Errorf("scan old row: %w", err)
		}

		record, err := common.LoadENR(nodeRecord)
		if err != nil {
			return fmt.Errorf("parse node: %w", err)
		}

		pubKey, err := common.RecordPubKey(record)
		if err != nil {
			return fmt.Errorf("loading pub key: %w", err)
		}

		_, err = stmt.Exec(
			nodeID,
			common.PubkeyBytes(pubKey),
			&nodeRecord,
			ipAddress,
			firstFound,
			lastFound,
			nextCrawl,
		)
		if err != nil {
			return fmt.Errorf("insert row: %w", err)
		}
	}

	_, err = tx.Exec("DROP TABLE discovered_nodes_old")
	if err != nil {
		return fmt.Errorf("drop old table: %w", err)
	}

	return nil
}

func migrationCrawler004NodeType(tx *sql.Tx) error {
	_, err := tx.Exec("ALTER TABLE discovered_nodes RENAME TO discovered_nodes_old")
	if err != nil {
		return fmt.Errorf("renaming table: %w", err)
	}

	_, err = tx.Exec(`
		CREATE TABLE discovered_nodes (
			node_id		BLOB	PRIMARY KEY,
			node_type	INTEGER NOT NULL,
			node_pubkey	BLOB	NOT NULL,
			node_record	BLOB	NOT NULL,
			ip_address	TEXT	NOT NULL,
			first_found	INTEGER	NOT NULL,
			last_found	INTEGER	NOT NULL,
			next_crawl	INTEGER	NOT NULL
		) STRICT;
	`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	oldRows, err := tx.Query(`
		SELECT
			node_id,
			node_pubkey,
			node_record,
			ip_address,
			first_found,
			last_found,
			next_crawl
		FROM discovered_nodes_old
	`)
	if err != nil {
		return fmt.Errorf("querying old nodes: %w", err)
	}
	defer oldRows.Close()

	stmt, err := tx.Prepare(`
		INSERT INTO discovered_nodes (
			node_id,
			node_type,
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
			?,
			?,
			?,
			?
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for oldRows.Next() {
		var nodeID, nodePubkey, nodeRecord []byte
		var ipAddress string
		var firstFound, lastFound, nextCrawl int64

		err := oldRows.Scan(
			&nodeID,
			&nodePubkey,
			&nodeRecord,
			&ipAddress,
			&firstFound,
			&lastFound,
			&nextCrawl,
		)
		if err != nil {
			return fmt.Errorf("scan old row: %w", err)
		}

		record, err := common.LoadENR(nodeRecord)
		if err != nil {
			return fmt.Errorf("parse node: %w", err)
		}

		_, err = stmt.Exec(
			nodeID,
			common.ENRNodeType(record),
			nodePubkey,
			nodeRecord,
			ipAddress,
			firstFound,
			lastFound,
			nextCrawl,
		)
		if err != nil {
			return fmt.Errorf("insert row: %w", err)
		}
	}

	_, err = tx.Exec("DROP TABLE discovered_nodes_old")
	if err != nil {
		return fmt.Errorf("drop old table: %w", err)
	}

	return nil
}
