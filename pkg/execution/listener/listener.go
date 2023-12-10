package listener

import (
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

	ch        chan common.NodeJSON
	wg        *sync.WaitGroup
	listeners []net.Listener
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

		ch:        make(chan common.NodeJSON, 64),
		wg:        new(sync.WaitGroup),
		listeners: make([]net.Listener, 0, len(nodeKeys)),
	}
}

func (l *Listener) Wait() {
	l.wg.Wait()
}

func (l *Listener) Close() {
	for _, listener := range l.listeners {
		listener.Close()
	}

	close(l.ch)
}

func (l *Listener) StartDaemon() {
	for i, nodeKey := range l.nodeKeys {
		port := l.listenPortStart + uint16(i)
		l.wg.Add(1)
		go l.startListener(
			nodeKey,
			fmt.Sprintf(
				"[%s]:%d",
				l.listenHost,
				port,
			),
			port,
		)
	}

	l.wg.Add(1)
	go l.updaterLoop()
}

func (l *Listener) updaterLoop() {
	defer l.wg.Done()

	for node := range l.ch {
		metrics.NodeUpdateBacklog("listener", len(l.ch))

		err := l.db.UpsertCrawledNode(node)
		if err != nil {
			log.Error("upsert crawled node failed", "err", err, "node_id", node.TerminalString())
		}

		metrics.NodeUpdateInc(string(node.Direction), node.Error)
	}
}

func (l *Listener) startListener(nodeKey *ecdsa.PrivateKey, listenAddr string, port uint16) {
	defer l.wg.Done()

	disc, err := disc.New(l.db, nodeKey, listenAddr, port)
	if err != nil {
		log.Error("new discovery failed", "err", err, "addr", listenAddr)

		return
	}
	defer disc.Close()

	err = disc.StartDaemon()
	if err != nil {
		log.Error("start discovery failed", "err", err, "addr", listenAddr)

		return
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Error("listener failed", "err", err)

		return
	}

	l.listeners = append(l.listeners, listener)

	for {
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

		go l.crawlPeer(nodeKey, conn)
	}
}

func (l *Listener) crawlPeer(nodeKey *ecdsa.PrivateKey, fd net.Conn) {
	pubKey, conn, err := p2p.Accept(nodeKey, fd)
	if err != nil {
		known, _ := p2p.TranslateError(err)
		if !known {
			log.Info("accept peer failed", "err", err, "ip", fd.RemoteAddr().String())
		}

		return
	}
	defer conn.Close()

	l.ch <- conn.GetClientInfo(
		l.nodeFromConn(pubKey, fd),
		common.DirectionAccept,
		l.db.GetMissingBlock,
	)
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
