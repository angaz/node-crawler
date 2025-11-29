package disc

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"net"
	"sync"
	"time"

	"log/slog"

	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/jackc/pgx/v5"
)

type Discovery struct {
	db         *database.DB
	listenAddr string

	privateKey *ecdsa.PrivateKey

	nodeDB    *enode.DB
	localnode *enode.LocalNode

	v4 *discover.UDPv4
	v5 *discover.UDPv5
	wg *sync.WaitGroup
}

func New(
	db *database.DB,
	privateKey *ecdsa.PrivateKey,
	listenAddr string,
	port uint16,
) (*Discovery, error) {
	d := &Discovery{
		db:         db,
		listenAddr: listenAddr,
		privateKey: privateKey,

		nodeDB:    &enode.DB{},
		localnode: &enode.LocalNode{},
		v4:        &discover.UDPv4{},
		v5:        &discover.UDPv5{},
		wg:        &sync.WaitGroup{},
	}

	var err error

	nodeDB, err := enode.OpenDB("") // In memory
	if err != nil {
		return nil, fmt.Errorf("opening enode DB failed: %w", err)
	}

	d.nodeDB = nodeDB
	d.localnode = enode.NewLocalNode(nodeDB, d.privateKey)
	d.localnode.Set(enr.TCP(port))

	err = d.setupDiscovery()
	if err != nil {
		return nil, fmt.Errorf("discovery setup: %w", err)
	}

	return d, nil
}

func (d *Discovery) DiscV4() *discover.UDPv4 {
	return d.v4
}

func (d *Discovery) DiscV5() *discover.UDPv5 {
	return d.v5
}

func (d *Discovery) setupDiscovery() error {
	addr, err := net.ResolveUDPAddr("udp", d.listenAddr)
	if err != nil {
		return fmt.Errorf("resolving udp address failed: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listening udp failed: %w", err)
	}

	unhandled := make(chan discover.ReadPacket, 128)
	sharedConn := &sharedUDPConn{conn, unhandled}

	//nolint:exhaustruct
	d.v4, err = discover.ListenV4(conn, d.localnode, discover.Config{
		PrivateKey: d.privateKey,
		Bootnodes:  v4Bootnodes,
		Unhandled:  unhandled,
	})
	if err != nil {
		return fmt.Errorf("setting up discv4 failed: %w", err)
	}

	//nolint:exhaustruct
	d.v5, err = discover.ListenV5(sharedConn, d.localnode, discover.Config{
		PrivateKey: d.privateKey,
		Bootnodes:  v5Bootnodes,
	})
	if err != nil {
		return fmt.Errorf("setting up discv5 failed: %w", err)
	}

	return nil
}

func (d *Discovery) Close() {
	d.v4.Close()
	d.v5.Close()
}

func (d *Discovery) Wait() {
	d.wg.Wait()
}

func (d *Discovery) crawlNodeV4(ctx context.Context, tx pgx.Tx, node *enode.Node) error {
	defer metrics.DiscCrawlCount.WithLabelValues("v4").Inc()

	id := node.ID()

	result := d.v4.LookupPubkey(node.Pubkey())

	var found bool

	for _, rn := range result {
		err := d.db.UpsertNode(ctx, tx, rn)
		if err != nil {
			return fmt.Errorf("upsert node v4 lookup: %w", err)
		}

		if rn.ID() == id {
			found = true
		}
	}

	if found {
		return nil
	}

	err := d.db.UpdateDiscNodeFailed(ctx, tx, node.ID())
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	return nil
}

func (d *Discovery) crawlNodeV5(ctx context.Context, tx pgx.Tx, id enode.ID) error {
	defer metrics.DiscCrawlCount.WithLabelValues("v5").Inc()

	result := d.v5.Lookup(id)

	var found bool

	for _, rn := range result {
		err := d.db.UpsertNode(ctx, tx, rn)
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

	err := d.db.UpdateDiscNodeFailed(ctx, tx, id)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	return nil
}

func (d *Discovery) crawlNode(ctx context.Context, tx pgx.Tx, node *enode.Node) error {
	if common.IsEnode(node.Record()) {
		err := d.crawlNodeV4(ctx, tx, node)
		if err != nil {
			return fmt.Errorf("crawl node v4: %w", err)
		}

		return nil
	}

	err := d.crawlNodeV5(ctx, tx, node.ID())
	if err != nil {
		return fmt.Errorf("crawl node v5: %w", err)
	}

	return nil
}

func closestDisc(discs []*Discovery, nodeID enode.ID) *Discovery {
	var lastDisc = discs[0]
	var lastDistance = enode.LogDist(lastDisc.localnode.ID(), nodeID)

	for _, disc := range discs[1:] {
		discID := disc.localnode.ID()

		distance := enode.LogDist(discID, nodeID)

		if distance < lastDistance {
			lastDisc = disc
			lastDistance = distance
		}
	}

	return lastDisc
}

func (d *Discovery) discCrawler(ctx context.Context, discs []*Discovery) {
	defer d.wg.Done()

	for ctx.Err() == nil {
		node, err := d.db.DiscNodesToCrawl(ctx)
		if err != nil {
			slog.Error("disc crawl select node failed", "err", err)
		}

		disc := closestDisc(discs, node.ID())

		err = d.db.WithTxAsync(
			ctx,
			database.TxOptionsDeferrable,
			func(ctx context.Context, tx pgx.Tx) error {
				return disc.crawlNode(ctx, tx, node)
			},
		)
		if err != nil {
			slog.Error("disc crawl node failed", "err", err)
		}
	}
}

func (d *Discovery) randomLoop(ctx context.Context, iter enode.Iterator, discVersion string) {
	defer d.wg.Done()

	for iter.Next() {
		err := d.db.WithTxAsync(
			ctx,
			database.TxOptionsDeferrable,
			func(ctx context.Context, tx pgx.Tx) error {
				return d.db.UpsertNode(ctx, tx, iter.Node())
			},
		)
		if err != nil {
			slog.Error("upserting disc node failed", "err", err)
		}

		metrics.DiscUpdateCount.WithLabelValues(discVersion).Inc()

		time.Sleep(30 * time.Second)
	}
}

func (d *Discovery) StartDaemon(ctx context.Context, discs []*Discovery) {
	d.wg.Add(1)

	go d.discCrawler(ctx, discs)
}

// Starts a random discovery crawler in a goroutine
func (d *Discovery) StartRandomDaemon(ctx context.Context) {
	d.wg.Add(2)

	go d.randomLoop(ctx, d.v4.RandomNodes(), "v4")
	go d.randomLoop(ctx, d.v5.RandomNodes(), "v5")
}
