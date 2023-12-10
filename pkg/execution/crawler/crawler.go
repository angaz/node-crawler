package crawler

import (
	"crypto/ecdsa"
	"math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/execution/p2p"
	"github.com/ethereum/node-crawler/pkg/fifomemory"
	"github.com/ethereum/node-crawler/pkg/metrics"
)

type Crawler struct {
	db       *database.DB
	nodeKeys []*ecdsa.PrivateKey
	workers  int

	toCrawl chan *enode.Node
	ch      chan common.NodeJSON
	wg      *sync.WaitGroup
}

func New(
	db *database.DB,
	nodeKeys []*ecdsa.PrivateKey,
	workers int,
) (*Crawler, error) {
	c := &Crawler{
		db:       db,
		nodeKeys: nodeKeys,
		workers:  workers,

		toCrawl: make(chan *enode.Node),
		ch:      make(chan common.NodeJSON, 64),
		wg:      new(sync.WaitGroup),
	}

	return c, nil
}

func (c *Crawler) Wait() {
	c.wg.Wait()
}

func (c *Crawler) Close() {
	close(c.toCrawl)
	close(c.ch)
}

func (c *Crawler) StartDaemon() error {
	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.crawler()
	}

	c.wg.Add(1)
	go c.nodesToCrawlDaemon(c.workers * 4)

	c.wg.Add(1)
	go c.updaterLoop()

	return nil
}

func (c *Crawler) updaterLoop() {
	defer c.wg.Done()

	for node := range c.ch {
		metrics.NodeUpdateBacklog("crawler", len(c.ch))

		err := c.db.UpsertCrawledNode(node)
		if err != nil {
			log.Error("upsert crawled node failed", "err", err, "node_id", node.TerminalString())
		}

		metrics.NodeUpdateInc(string(node.Direction), node.Error)
	}
}

func (c *Crawler) randomNodeKey() *ecdsa.PrivateKey {
	idx := rand.Intn(len(c.nodeKeys))

	return c.nodeKeys[idx]
}

func (c *Crawler) crawlNode(node *enode.Node) {
	conn, err := p2p.Dial(c.randomNodeKey(), node, 10*time.Second)
	if err != nil {
		known, errStr := p2p.TranslateError(err)
		if !known {
			log.Info("dial failed", "err", err)
		}

		//nolint:exhaustruct  // Missing values because of error.
		c.ch <- common.NodeJSON{
			N:         node,
			EthNode:   true,
			Direction: common.DirectionDial,
			Error:     errStr,
		}

		return
	}
	defer conn.Close()

	c.ch <- conn.GetClientInfo(
		node,
		common.DirectionDial,
		c.db.GetMissingBlock,
	)
}

// Meant to be run as a goroutine
//
// Selects nodes to crawl from the database
func (c *Crawler) nodesToCrawlDaemon(batchSize int) {
	defer c.wg.Done()

	// To make sure we don't crawl the same node too often.
	recentlyCrawled := fifomemory.New[enode.ID](batchSize * 2)

	for {
		nodes, err := c.db.SelectDiscoveredNodeSlice(batchSize)
		if err != nil {
			log.Error("selecting discovered node slice failed", "err", err)
			time.Sleep(time.Minute)

			continue
		}

		for _, node := range nodes {
			nodeID := node.ID()

			if !recentlyCrawled.Contains(nodeID) {
				recentlyCrawled.Push(nodeID)
				c.toCrawl <- node
			}
		}

		if len(nodes) < batchSize {
			time.Sleep(time.Minute)
		}
	}
}

// Meant to be run as a goroutine
//
// Crawls nodes from the toCrawl channel
func (c *Crawler) crawler() {
	defer c.wg.Done()

	for node := range c.toCrawl {
		c.crawlNode(node)
	}
}
