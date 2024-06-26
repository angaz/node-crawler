package listener

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"net"
	"sync"
	"time"

	"log/slog"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/execution/disc"
	"github.com/ethereum/node-crawler/pkg/execution/p2p"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/jackc/pgx/v5"
)

type Listener struct {
	db              *database.DB
	nodeKeys        []*ecdsa.PrivateKey
	listenHost      string
	listenPortStart uint16

	wg        *sync.WaitGroup
	listeners []net.Listener
	disc      []*disc.Discovery
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

		wg:        new(sync.WaitGroup),
		listeners: make([]net.Listener, 0, len(nodeKeys)),
		disc:      make([]*disc.Discovery, 0, len(nodeKeys)),
	}
}

func (l *Listener) Wait() {
	l.wg.Wait()
}

func (l *Listener) Close() {
	for _, listener := range l.listeners {
		listener.Close()
	}

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
			port,
		)
	}
}

func (l *Listener) StartDiscCrawlers(ctx context.Context, crawlers int) {
	if len(l.disc) == 0 {
		slog.Error("start disc crawlers: number of discovery servers running is zero")

		return
	}

	for i := 0; i < crawlers; i++ {
		disc := l.disc[i%len(l.disc)]

		disc.StartRandomDaemon(ctx)
		disc.StartDaemon(ctx, l.disc)
	}
}

func (l *Listener) startListener(ctx context.Context, nodeKey *ecdsa.PrivateKey, listenAddr string, port uint16) {
	disc, err := disc.New(l.db, nodeKey, listenAddr, port)
	if err != nil {
		slog.Error("new discovery failed", "err", err, "addr", listenAddr)

		return
	}

	l.disc = append(l.disc, disc)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		slog.Error("listener failed", "err", err)

		return
	}

	l.listeners = append(l.listeners, listener)

	l.wg.Add(1)

	go func() {
		defer l.wg.Done()

		for ctx.Err() == nil {
			var conn net.Conn
			var err error

			for {
				conn, err = listener.Accept()
				if netutil.IsTemporaryError(err) {
					time.Sleep(100 * time.Millisecond)
					continue
				} else if err != nil {
					slog.Error("crawler listener accept failed", "err", err)
				}

				break
			}

			metrics.AcceptedConnections.WithLabelValues(listenAddr).Inc()

			go l.crawlPeer(ctx, nodeKey, conn)
		}
	}()
}

func (l *Listener) crawlPeer(ctx context.Context, nodeKey *ecdsa.PrivateKey, fd net.Conn) {
	pubKey, conn, err := p2p.Accept(nodeKey, fd)
	if err != nil {
		known, _ := p2p.TranslateError(err)
		if !known {
			slog.Info("accept peer failed", "err", err, "ip", fd.RemoteAddr().String())
		}

		return
	}
	defer conn.Close()

	err = l.db.WithTxAsync(
		ctx,
		database.TxOptionsDeferrable,
		func(ctx context.Context, tx pgx.Tx) error {
			node := conn.GetClientInfo(
				ctx,
				tx,
				l.nodeFromConn(pubKey, fd),
				common.DirectionAccept,
				l.db.GetMissingBlock,
			)

			err = l.db.UpsertCrawledNode(ctx, tx, node)
			if err != nil {
				return fmt.Errorf("upsert: %s: %w", node.TerminalString(), err)
			}

			return nil
		},
	)
	if err != nil {
		slog.Error("accept peer failed", "err", err)
	}
}

func (l *Listener) nodeFromConn(pubkey *ecdsa.PublicKey, conn net.Conn) *enode.Node {
	var ip net.IP
	var port int

	tcp, ok := conn.RemoteAddr().(*net.TCPAddr)
	if ok {
		ip = tcp.IP
		port = tcp.Port
	}

	node := enode.NewV4(pubkey, ip, port, port)

	return node
}
