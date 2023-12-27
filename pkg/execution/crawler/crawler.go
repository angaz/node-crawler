package crawler

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/execution/p2p"
	"github.com/jackc/pgx/v5"
)

type Crawler struct {
	db       *database.DB
	nodeKeys []*ecdsa.PrivateKey

	wg *sync.WaitGroup
}

var (
	ErrNothingToCrawl = errors.New("nothing to crawl")
)

func New(
	db *database.DB,
	nodeKeys []*ecdsa.PrivateKey,
) (*Crawler, error) {
	c := &Crawler{
		db:       db,
		nodeKeys: nodeKeys,

		wg: new(sync.WaitGroup),
	}

	return c, nil
}

func (c *Crawler) Wait() {
	c.wg.Wait()
}

func (c *Crawler) Close() {}

func (c *Crawler) StartDaemon(ctx context.Context, workers int) error {
	for i := 0; i < workers; i++ {
		c.wg.Add(1)
		go c.crawler(ctx)
	}

	return nil
}

func (c *Crawler) randomNodeKey() *ecdsa.PrivateKey {
	idx := rand.Intn(len(c.nodeKeys))

	return c.nodeKeys[idx]
}

func (c *Crawler) crawlNode(ctx context.Context, tx pgx.Tx, node *enode.Node) error {
	conn, err := p2p.Dial(c.randomNodeKey(), node, 10*time.Second)
	if err != nil {
		known, errStr := p2p.TranslateError(err)
		if !known {
			log.Error("dial failed", "err", err)
		}

		//nolint:exhaustruct  // Missing values because of error.
		err := c.db.UpsertCrawledNode(ctx, tx, common.NodeJSON{
			N:         node,
			EthNode:   true,
			Direction: common.DirectionDial,
			Error:     errStr,
		})
		if err != nil {
			return fmt.Errorf("upsert err: %w", err)
		}

		return nil
	}
	defer conn.Close()

	err = c.db.UpsertCrawledNode(ctx, tx, conn.GetClientInfo(
		ctx,
		tx,
		node,
		common.DirectionDial,
		c.db.GetMissingBlock,
	))
	if err != nil {
		return fmt.Errorf("upsert success: %w", err)
	}

	return nil
}

func (c *Crawler) crawlAndUpdateNode(ctx context.Context) error {
	return c.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		node, err := c.db.SelectDiscoveredNode(ctx, tx)
		if err != nil {
			return fmt.Errorf("select node: %w", err)
		}

		if node == nil {
			return ErrNothingToCrawl
		}

		err = c.crawlNode(ctx, tx, node)
		if err != nil {
			return fmt.Errorf("crawl node: %w", err)
		}

		return nil
	})
}

// Meant to be run as a goroutine
func (c *Crawler) crawler(ctx context.Context) {
	defer c.wg.Done()

	for ctx.Err() == nil {
		err := c.crawlAndUpdateNode(ctx)
		if err != nil {
			if !errors.Is(err, ErrNothingToCrawl) {
				log.Error("crawl failed", "err", err)
			}

			time.Sleep(time.Minute)

			continue
		}
	}
}
