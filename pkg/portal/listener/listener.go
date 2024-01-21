package listener

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/portal/disc"
)

type Listener struct {
	db *database.DB

	nodeKeys        []*ecdsa.PrivateKey
	listenHost      string
	listenPortStart uint16

	wg   *sync.WaitGroup
	disc []*disc.Discovery
}

func New(
	db *database.DB,
	nodeKeys []*ecdsa.PrivateKey,
	listenHost string,
	listenPortStart uint16,
) *Listener {
	return &Listener{
		db:              db,
		nodeKeys:        nodeKeys,
		listenHost:      listenHost,
		listenPortStart: listenPortStart,
		wg:              &sync.WaitGroup{},
		disc:            []*disc.Discovery{},
	}
}

func (l *Listener) Wait() {
	l.wg.Wait()
}

func (l *Listener) Close() {
	for _, disc := range l.disc {
		disc.Close()
	}
}

func (l *Listener) StartDaemon(ctx context.Context) {
	for i, nodeKey := range l.nodeKeys {
		port := l.listenPortStart + uint16(i)

		l.startListener(
			ctx,
			nodeKey,
			fmt.Sprintf("[%s]:%d", l.listenHost, port),
		)
	}
}

func (l *Listener) StartDiscCrawlers(ctx context.Context, crawlers int) {
	if len(l.disc) == 0 {
		slog.Error("start portal disc crawlers: number of crawlers is zero")

		return
	}

	for i := 0; i < crawlers; i++ {
		disc := l.disc[i%len(l.disc)]

		disc.StartDaemon(ctx)
	}
}

func (l *Listener) startListener(
	ctx context.Context,
	nodeKey *ecdsa.PrivateKey,
	listenAddr string,
) {
	disc, err := disc.New(l.db, nodeKey, listenAddr)
	if err != nil {
		slog.Error("new portal disc failed", "err", err, "addr", listenAddr)

		return
	}

	l.disc = append(l.disc, disc)
}
