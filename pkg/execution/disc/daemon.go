package disc

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/params"
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

func (d *Discovery) discLoop(iter enode.Iterator, discVersion string, ch chan<- *enode.Node) {
	defer d.wg.Done()

	for iter.Next() {
		ch <- iter.Node()

		metrics.DiscUpdateCount.WithLabelValues(discVersion).Inc()

		time.Sleep(250 * time.Microsecond)
	}
}

func (d *Discovery) updaterLoop(ch <-chan *enode.Node) {
	defer d.wg.Done()

	for {
		metrics.DiscUpdateBacklog.Set(float64(len(ch)))

		node := <-ch

		err := d.db.UpsertNode(node)
		if err != nil {
			log.Error("upserting disc node failed", "err", err)
		}
	}
}

// Starts the discovery in a goroutine
func (d *Discovery) StartDaemon() error {
	err := d.setupDiscovery()
	if err != nil {
		return fmt.Errorf("setting up discovery failed: %w", err)
	}

	ch := make(chan *enode.Node, 64)

	d.wg.Add(3)
	go d.discLoop(d.v4.RandomNodes(), "v4", ch)
	go d.discLoop(d.v5.RandomNodes(), "v5", ch)
	go d.updaterLoop(ch)

	return nil
}
