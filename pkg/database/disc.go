package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"
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

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
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

func (db *DB) UpsertNode(ctx context.Context, node *enode.Node) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("disc_upsert_node", start, err)

	ip := node.IP()

	location, err := db.IPToLocation(ip)
	if err != nil {
		return fmt.Errorf("ip to location: %w", err)
	}

	tx, err := db.pg.Begin(ctx)
	if err != nil {
		return fmt.Errorf("start tx: %w", err)
	}
	defer tx.Rollback(ctx)

	err = db.upsertCountryCity(ctx, tx, location)
	if err != nil {
		return fmt.Errorf("upsert country city: %w", err)
	}

	bestRecord, err := db.selectBestRecord(ctx, tx, node)
	if err != nil {
		return fmt.Errorf("select best record: %w", err)
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
			WHERE
				nodes.last_found < (now() - INTERVAL '6 hours')  -- Only update once every 6 hours
				OR nodes.node_record != excluded.node_record
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

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

type NodeToCrawl struct {
	NextCrawl time.Time
	Enode     *enode.Node
}

func (db *DB) fetchNodesToCrawl(ctx context.Context) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("select_disc_node", start, err)

	rows, err := db.pg.Query(
		ctx,
		`
			SELECT
				next_crawl,
				node_record
			FROM disc.nodes
			WHERE
				node_type IN ('Unknown', 'Execution')
			ORDER BY next_crawl
			LIMIT 8196
		`,
	)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var nextCrawl time.Time
	var enrBytes []byte

	_, err = pgx.ForEachRow(rows, []any{&nextCrawl, &enrBytes}, func() error {
		record, err := common.LoadENR(enrBytes)
		if err != nil {
			return fmt.Errorf("load enr: %w", err)
		}

		node, err := common.RecordToEnode(record)
		if err != nil {
			return fmt.Errorf("record to enode: %w, %x", err, enrBytes)
		}

		db.nodesToCrawlCache <- &NodeToCrawl{
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

func (db *DB) NodesToCrawl(ctx context.Context) (*enode.Node, error) {
	db.nodesToCrawlLock.Lock()
	defer db.nodesToCrawlLock.Unlock()

	for ctx.Err() == nil {
		select {
		case nextNode := <-db.nodesToCrawlCache:
			if nextNode == nil {
				log.Info("next node is null")

				continue
			}

			if db.recentlyCrawled.ContainsOrPush(nextNode.Enode.ID()) {
				log.Info("recently crawled", "node", nextNode.Enode.ID().TerminalString())

				continue
			}

			sleepDur := time.Until(nextNode.NextCrawl)
			if sleepDur > 0 {
				log.Info("sleeping", "until", nextNode.NextCrawl)

				time.Sleep(sleepDur)
			}

			return nextNode.Enode, nil
		default:
			err := db.fetchNodesToCrawl(ctx)
			if err != nil {
				log.Error("fetch nodes to crawl failed", "err", err)
				time.Sleep(time.Minute)
			}
		}
	}

	return nil, ctx.Err()
}
