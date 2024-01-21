package disc

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/ethereum/node-crawler/pkg/version"
	"github.com/jackc/pgx/v5"
)

type Discovery struct {
	db         *database.DB
	listenAddr string
	privateKey *ecdsa.PrivateKey

	nodeDB    *enode.DB
	localNode *enode.LocalNode
	disc      *discover.UDPv5

	wg *sync.WaitGroup
}

func New(
	db *database.DB,
	privateKey *ecdsa.PrivateKey,
	listenAddr string,
) (*Discovery, error) {
	nodeDB, err := enode.OpenDB("") // In memory
	if err != nil {
		return nil, fmt.Errorf("opening enode DB failed: %w", err)
	}

	localNode := enode.NewLocalNode(nodeDB, privateKey)
	localNode.Set(
		common.ClientName(
			version.ClientName("NodeCrawler"),
		),
	)

	d := &Discovery{
		db:         db,
		listenAddr: listenAddr,
		privateKey: privateKey,

		nodeDB:    nodeDB,
		localNode: localNode,
		disc:      nil,

		wg: &sync.WaitGroup{},
	}

	err = d.setupDiscovery()
	if err != nil {
		return nil, fmt.Errorf("setup: %w", err)
	}

	return d, nil
}

func (d *Discovery) Close() {
	d.disc.Close()
}

func (d *Discovery) setupDiscovery() error {
	addr, err := net.ResolveUDPAddr("udp", d.listenAddr)
	if err != nil {
		return fmt.Errorf("resolving udp address: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listening udp: %w", err)
	}

	//nolint:exhaustruct
	d.disc, err = discover.ListenV5(conn, d.localNode, discover.Config{
		PrivateKey: d.privateKey,
		Bootnodes:  bootnodes,
	})
	if err != nil {
		return fmt.Errorf("setting up discv5: %w", err)
	}

	return nil
}

func (d *Discovery) crawlNode(ctx context.Context, tx pgx.Tx, id enode.ID) error {
	defer metrics.PortalDiscCrawlCount.Inc()

	result := d.disc.Lookup(id)

	var found bool

	for _, rn := range result {
		err := d.db.UpsertPortalDiscNode(ctx, tx, rn)
		if err != nil {
			return fmt.Errorf("upsert node lookup: %w", err)
		}

		if rn.ID() == id {
			found = true
		}
	}

	if found {
		return nil
	}

	err := d.db.UpdatePortalDiscNodeFailed(ctx, tx, id)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	return nil
}

func randomID() enode.ID {
	var id enode.ID

	_, _ = rand.Read(id[:])

	return id
}

func (d *Discovery) crawler(ctx context.Context) {
	defer d.wg.Done()

	err := d.db.WithTxAsync(
		ctx,
		database.TxOptionsDeferrable,
		func(ctx context.Context, tx pgx.Tx) error {
			return d.crawlNode(ctx, tx, randomID())
		},
	)
	if err != nil {
		slog.Error("portal disc crawl random node failed", "err", err)
	}

	for ctx.Err() == nil {
		func() {
			nodeID, err := d.db.PortalDiscToCrawl(ctx)
			if err != nil {
				slog.Error("portal disc crawl select node failed", "err", err)

				return
			}
			defer d.db.PortalDiscCrawlDone(nodeID)

			err = d.db.WithTxAsync(
				ctx,
				database.TxOptionsDeferrable,
				func(ctx context.Context, tx pgx.Tx) error {
					err := d.crawlNode(ctx, tx, nodeID)
					if err != nil {
						return fmt.Errorf("crawl node: %w", err)
					}

					err = d.crawlNode(ctx, tx, randomID())
					if err != nil {
						return fmt.Errorf("crawl random node: %w", err)
					}

					return nil
				},
			)
			if err != nil {
				slog.Error("portal disc crawl node failed", "err", err)
			}
		}()
	}
}

func (d *Discovery) StartDaemon(ctx context.Context) {
	d.wg.Add(1)

	go d.crawler(ctx)
}
