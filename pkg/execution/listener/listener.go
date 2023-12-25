package listener

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/execution/disc"
	"github.com/ethereum/node-crawler/pkg/execution/p2p"
	"github.com/ethereum/node-crawler/pkg/metrics"
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
}

func (l *Listener) StartDaemon(ctx context.Context) {
	for i, nodeKey := range l.nodeKeys {
		port := l.listenPortStart + uint16(i)

		l.wg.Add(1)

		go l.startListener(
			ctx,
			nodeKey,
			fmt.Sprintf("[%s]:%d", l.listenHost, port),
			port,
		)
	}
}

func (l *Listener) StartDiscCrawlers(ctx context.Context, crawlers int) {
	for i := 0; i < crawlers; i++ {
		disc := l.disc[i%len(l.disc)]

		disc.StartDaemon(ctx)
	}
}

func (l *Listener) startListener(ctx context.Context, nodeKey *ecdsa.PrivateKey, listenAddr string, port uint16) {
	defer l.wg.Done()

	disc, err := disc.New(l.db, nodeKey, listenAddr, port)
	if err != nil {
		log.Error("new discovery failed", "err", err, "addr", listenAddr)

		return
	}
	defer disc.Close()

	l.disc = append(l.disc, disc)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Error("listener failed", "err", err)

		return
	}

	l.listeners = append(l.listeners, listener)

	for ctx.Err() == nil {
		var conn net.Conn
		var err error

		for {
			conn, err = listener.Accept()
			if netutil.IsTemporaryError(err) {
				time.Sleep(100 * time.Millisecond)
				continue
			} else if err != nil {
				log.Error("crawler listener accept failed", "err", err)
			}

			break
		}

		metrics.AcceptedConnections.WithLabelValues(listenAddr).Inc()

		go l.crawlPeer(ctx, nodeKey, conn)
	}
}

func (l *Listener) crawlPeer(ctx context.Context, nodeKey *ecdsa.PrivateKey, fd net.Conn) {
	pubKey, conn, err := p2p.Accept(nodeKey, fd)
	if err != nil {
		known, _ := p2p.TranslateError(err)
		if !known {
			log.Info("accept peer failed", "err", err, "ip", fd.RemoteAddr().String())
		}

		return
	}
	defer conn.Close()

	tx, err := l.db.Begin(ctx)
	if err != nil {
		log.Error("accept crawl peer begin failed", "err", err)

		return
	}
	defer tx.Rollback(ctx)

	node := conn.GetClientInfo(
		ctx,
		tx,
		l.nodeFromConn(pubKey, fd),
		common.DirectionAccept,
		l.db.GetMissingBlock,
	)

	err = l.db.UpsertCrawledNode(ctx, tx, node)
	if err != nil {
		log.Error("accept crawl peer upsert failed", "err", err, "node_id", node.TerminalString())

		return
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Error("accept crawl peer commit failed", "err", err, "node_id", node.TerminalString())

		return
	}

	metrics.NodeUpdateInc(string(node.Direction), node.Error)
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
