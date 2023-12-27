package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/node-crawler/pkg/common"
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

func (_ *DB) upsertCountryCity(ctx context.Context, db execer, location location) error {
	_, err := db.Exec(
		ctx,
		`
			WITH country AS (
				INSERT INTO geoname.countries (
					country_geoname_id,
					country_name
				)
				VALUES (
					@country_geoname_id,
					@country_name
				)
				ON CONFLICT (country_geoname_id) DO NOTHING
			)

			INSERT INTO geoname.cities (
				city_geoname_id,
				city_name,
				country_geoname_id,
				latitude,
				longitude
			)
			VALUES (
				@city_geoname_id,
				@city_name,
				@country_geoname_id,
				@latitude,
				@longitude
			)
			ON CONFLICT (city_geoname_id) DO NOTHING
		`,
		pgx.NamedArgs{
			"country_geoname_id": location.countryGeoNameID,
			"country_name":       location.country,
			"city_geoname_id":    location.cityGeoNameID,
			"city_name":          location.city,
			"latitude":           location.latitude,
			"longitude":          location.longitude,
		},
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func (db *DB) UpsertNode(ctx context.Context, node *enode.Node) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("disc_upsert_node", start, err)

	ip := node.IP()

	location, err := db.IPToLocation(ip)
	if err != nil {
		return fmt.Errorf("ip to location: %w", err)
	}

	err = db.upsertCountryCity(ctx, db.pg, location)
	if err != nil {
		return fmt.Errorf("upsert country city: %w", err)
	}

	var savedRecordBytes []byte
	var savedRecord *enr.Record

	row := db.pg.QueryRow(
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

	err = row.Scan(&savedRecordBytes)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("query node_record: %w", err)
	}

	if savedRecordBytes != nil {
		savedRecord, err = common.LoadENR(savedRecordBytes)
		if err != nil {
			return fmt.Errorf("load saved record: %w", err)
		}
	}

	bestRecord := common.BestRecord(savedRecord, node.Record())

	_, err = db.pg.Exec(
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
			) VALUES (
				@node_id,
				@node_type,
				now(),
				now(),
				now(),
				@node_pubkey,
				@node_record,
				@ip_address,
				@city_geoname_id
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				node_type = excluded.node_type,
				last_found = now(),
				node_record = excluded.node_record,
				ip_address = excluded.ip_address,
				city_geoname_id = excluded.city_geoname_id
			WHERE nodes.last_found < (now() - INTERVAL '6 hours')  -- Only update once every 6 hours
		`,
		pgx.NamedArgs{
			"node_id":         node.ID().Bytes(),
			"node_type":       common.ENRNodeType(node.Record()),
			"node_pubkey":     common.PubkeyBytes(node.Pubkey()),
			"node_record":     common.EncodeENR(bestRecord),
			"ip_address":      ip.String(),
			"city_geoname_id": location.cityGeoNameID,
		},
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func (_ *DB) SelectDiscoveredNode(ctx context.Context, tx pgx.Tx) (*enode.Node, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("select_disc_node_slice", start, err)

	rows, err := tx.Query(
		ctx,
		`
			SELECT
				node_record
			FROM disc.nodes
			WHERE
				next_crawl < now()
				AND node_type IN ('Unknown', 'Execution')
			ORDER BY next_crawl ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	var enrBytes []byte

	err = rows.Scan(&enrBytes)
	if err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	record, err := common.LoadENR(enrBytes)
	if err != nil {
		return nil, fmt.Errorf("load enr: %w", err)
	}

	node, err := common.RecordToEnode(record)
	if err != nil {
		return nil, fmt.Errorf("record to enode: %w, %x", err, enrBytes)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows iteration failed: %w", err)
	}

	return node, nil
}
