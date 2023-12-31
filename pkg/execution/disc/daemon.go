package disc

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"net"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/metrics"
)

type Discovery struct {
	db         *database.DB
	listenAddr string

	privateKey *ecdsa.PrivateKey
	bootnodes  []*enode.Node

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

		bootnodes: []*enode.Node{},
		nodeDB:    &enode.DB{},
		localnode: &enode.LocalNode{},
		v4:        &discover.UDPv4{},
		v5:        &discover.UDPv5{},
		wg:        &sync.WaitGroup{},
	}

	var err error

	bootnodes := params.MainnetBootnodes
	d.bootnodes = make([]*enode.Node, len(bootnodes))

	for i, record := range bootnodes {
		d.bootnodes[i], err = enode.ParseV4(record)
		if err != nil {
			return nil, fmt.Errorf("parsing bootnode failed: %w", err)
		}
	}

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
		Bootnodes:  d.bootnodes,
		Unhandled:  unhandled,
	})
	if err != nil {
		return fmt.Errorf("setting up discv4 failed: %w", err)
	}

	//nolint:exhaustruct
	d.v5, err = discover.ListenV5(sharedConn, d.localnode, discover.Config{
		PrivateKey: d.privateKey,
		Bootnodes:  d.bootnodes,
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

func (d *Discovery) crawlNodeV4(ctx context.Context, node *enode.Node) error {
	defer metrics.DiscCrawlCount.WithLabelValues("v4").Inc()

	resp, err := d.v4.RequestENR(node)
	if err == nil {
		err = d.db.UpsertNode(ctx, resp)
		if err != nil {
			return fmt.Errorf("upsert node v4 request enr: %w", err)
		}

		return nil
	}

	var key enode.Secp256k1
	if node.Load(&key) != nil {
		log.Error("disc crawl node v4 failed", "err", "no secp256k1 key")

		return nil
	}

	id := node.ID()

	result := d.v4.LookupPubkey((*ecdsa.PublicKey)(&key))

	var found bool

	for _, rn := range result {
		err = d.db.UpsertNode(ctx, rn)
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

	err = d.db.UpdateDiscNodeFailed(ctx, node.ID())
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	return nil
}

func (d *Discovery) crawlNodeV5(ctx context.Context, node *enode.Node) error {
	defer metrics.DiscCrawlCount.WithLabelValues("v5").Inc()

	resp, err := d.v5.RequestENR(node)
	if err == nil {
		err = d.db.UpsertNode(ctx, resp)
		if err != nil {
			return fmt.Errorf("upsert node request enr: %w", err)
		}

		return nil
	}

	id := node.ID()

	result := d.v5.Lookup(id)

	var found bool

	for _, rn := range result {
		err = d.db.UpsertNode(ctx, rn)
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

	err = d.db.UpdateDiscNodeFailed(ctx, node.ID())
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	return nil
}

func (d *Discovery) crawlNode(ctx context.Context) error {
	node, err := d.db.DiscNodesToCrawl(ctx)
	if err != nil {
		return fmt.Errorf("select node: %w", err)
	}

	if node == nil {
		return nil
	}

	if common.IsEnode(node.Record()) {
		err = d.crawlNodeV4(ctx, node)
		if err != nil {
			return fmt.Errorf("crawl node v4: %w", err)
		}
	}

	err = d.crawlNodeV5(ctx, node)
	if err != nil {
		return fmt.Errorf("crawl node v5: %w", err)
	}

	return nil
}

func (d *Discovery) discCrawler(ctx context.Context) {
	defer d.wg.Done()

	for ctx.Err() == nil {
		err := d.crawlNode(ctx)
		if err != nil {
			log.Error("disc crawl node failed", "err", err)
		}
	}
}

func (d *Discovery) discLoop(ctx context.Context, iter enode.Iterator, discVersion string) {
	defer d.wg.Done()

	for iter.Next() {
		err := d.db.UpsertNode(ctx, iter.Node())
		if err != nil {
			log.Error("upserting disc node failed", "err", err)
		}

		metrics.DiscUpdateCount.WithLabelValues(discVersion).Inc()
	}
}

func (d *Discovery) StartDaemon(ctx context.Context) {
	d.wg.Add(1)

	go d.discCrawler(ctx)
}

// Starts a random discovery crawler in a goroutine
func (d *Discovery) StartRandomDaemon(ctx context.Context) {
	d.wg.Add(2)

	go d.discLoop(ctx, d.v4.RandomNodes(), "v4")
	go d.discLoop(ctx, d.v5.RandomNodes(), "v5")
}
