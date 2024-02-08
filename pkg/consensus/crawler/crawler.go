package crawler

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	consensustypes "github.com/ethereum/node-crawler/pkg/consensus/types"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/version"
	"github.com/jackc/pgx/v5"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	noise "github.com/libp2p/go-libp2p/p2p/security/noise"
	tcptransport "github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/pkg/errors"

	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
)

type Crawler struct {
	db    *database.DB
	hosts []*consensusHost

	wg *sync.WaitGroup
}

type consensusHost struct {
	host host.Host
}

var (
	ErrNothingToCrawl = errors.New("nothing to crawl")
)

func New(
	db *database.DB,
	nodeKeys []*ecdsa.PrivateKey,
) (*Crawler, error) {
	c := &Crawler{
		db:    db,
		hosts: make([]*consensusHost, len(nodeKeys)),

		wg: new(sync.WaitGroup),
	}

	for i, key := range nodeKeys {
		host, err := newHost(
			key,
			"0.0.0.0",
			9100+i,
			version.ClientName("nodecrawler"),
		)
		if err != nil {
			return nil, fmt.Errorf("new host: %w", err)
		}

		c.hosts[i] = host
	}

	return c, nil
}

func (c *Crawler) Wait() {
	c.wg.Wait()
}

func (_ *Crawler) Close() {}

func (c *Crawler) StartDeamon(ctx context.Context, workers int) error {
	for i := 0; i < workers; i++ {
		c.wg.Add(1)
		c.crawler(ctx)
	}

	return nil
}

func (c *Crawler) randomHost() *consensusHost {
	idx := rand.Intn(len(c.hosts))

	return c.hosts[idx]
}

func (c *Crawler) crawlNode(ctx context.Context, tx pgx.Tx, node *enode.Node) error {
	consensusNode, err := GetClientInfo(ctx, c.randomHost().host, node)
	if err != nil {
		return fmt.Errorf("get client info: %w", err)
	}

	fmt.Printf("%#v\n", consensusNode)

	return nil
}

// meant to be run as a goroutine.
func (c *Crawler) crawler(ctx context.Context) {
	defer c.wg.Done()

	for ctx.Err() == nil {
		err := c.crawlAndUpdateNode(ctx)
		if err != nil {
			if !errors.Is(err, ErrNothingToCrawl) {
				slog.Error("consensus crawl failed", "err", err)
			}

			time.Sleep(time.Minute)
		}

		os.Exit(1)
	}
}

func (c *Crawler) crawlAndUpdateNode(ctx context.Context) error {
	// node, err := c.db.ConsensusNodesToCrawl(ctx)
	// if err != nil {
	// 	return fmt.Errorf("select node: %w", err)
	// }

	node := enode.MustParse("enode://ec2bc17fdae2d41290ab0c9461c2dbc8981164f8e34e30321980cf6d129778bc09487526f93b4f3146880d7910c96980da9dc0ce7e2d9024add2d4a3140f4347@65.109.96.70:9000")

	if node == nil {
		return ErrNothingToCrawl
	}

	return c.db.WithTxAsync(
		ctx,
		database.TxOptionsDeferrable,
		func(ctx context.Context, tx pgx.Tx) error {
			err := c.crawlNode(ctx, tx, node)
			if err != nil {
				return fmt.Errorf("crawl node: %w", err)
			}

			return nil
		},
	)
}

// Stolen from: github.com/prysmaticlabs/prysm/v4/crypto/ecdsa/utils.go:ConvertToInterfacePrivkey
func PrivateKeyToP2P(privkey *ecdsa.PrivateKey) (crypto.PrivKey, error) {
	privBytes := privkey.D.Bytes()

	// In the event the number of bytes outputted by the big-int are less than 32,
	// we append bytes to the start of the sequence for the missing most significant
	// bytes.
	if len(privBytes) < 32 {
		privBytes = append(make([]byte, 32-len(privBytes)), privBytes...)
	}

	return crypto.UnmarshalSecp256k1PrivateKey(privBytes)
}

func newHost(
	nodeKey *ecdsa.PrivateKey,
	ip string,
	port int,
	userAgent string,
) (*consensusHost, error) {
	privKey, err := PrivateKeyToP2P(nodeKey)
	if err != nil {
		return nil, fmt.Errorf("convert private key: %w", err)
	}

	multiaddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ip, port))
	if err != nil {
		return nil, fmt.Errorf("new multiaddr: %w", err)
	}

	low := 500
	hi := 750

	connManager, err := connmgr.NewConnManager(
		low,
		hi,
		connmgr.WithGracePeriod(30*time.Minute),
	)
	if err != nil {
		return nil, fmt.Errorf("new connmgr: %w", err)
	}

	host, err := libp2p.New(
		libp2p.ListenAddrs(multiaddr),
		libp2p.Identity(privKey),
		libp2p.UserAgent(userAgent),
		libp2p.Transport(tcptransport.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connManager),
	)
	if err != nil {
		return nil, err
	}

	return &consensusHost{
		host: host,
	}, nil
}

// Stolen from: github.com/prysmaticlabs/prysm/v4/crypto/ecdsa/utils.go:ConvertToInterfacePubkey
func PubkeyToP2P(pubkey *ecdsa.PublicKey) (crypto.PubKey, error) {
	xVal, yVal := new(btcec.FieldVal), new(btcec.FieldVal)

	if xVal.SetByteSlice(pubkey.X.Bytes()) {
		return nil, errors.Errorf("X value overflows")
	}

	if yVal.SetByteSlice(pubkey.Y.Bytes()) {
		return nil, errors.Errorf("Y value overflows")
	}

	newKey := crypto.PubKey((*crypto.Secp256k1PublicKey)(btcec.NewPublicKey(xVal, yVal)))

	// Zero out temporary values.
	xVal.Zero()
	yVal.Zero()

	return newKey, nil
}

type Node struct {
	Client    common.Client
	Node      *enode.Node
	Error     string
	Direction common.Direction
}

func GetClientInfo(
	ctx context.Context,
	host host.Host,
	node *enode.Node,
) (*Node, error) {
	pubkey, err := PubkeyToP2P(node.Pubkey())
	if err != nil {
		return nil, fmt.Errorf("convert pubkey to p2p: %w", err)
	}

	id, err := peer.IDFromPublicKey(pubkey)
	if err != nil {
		return nil, fmt.Errorf("pubkey to id: %w", err)
	}

	port := node.TCP()
	if port == 0 {
		port = 9001
	}

	maddr, err := multiaddr.NewMultiaddr(
		fmt.Sprintf(
			"/ip4/%s/tcp/%d/p2p/%s",
			node.IP().String(),
			port,
			id.String(),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("new multiaddr: %w", err)
	}

	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return nil, fmt.Errorf("multiaddr to addr info: %w", err)
	}

	host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.TempAddrTTL)

	stream, err := host.NewStream(ctx, info.ID, "/eth2/beacon_chain/req/status/1/")
	if err != nil {
		return nil, fmt.Errorf("new stream: %w", err)
	}

	status := consensustypes.Status{
		ForkDigest:     []byte{},
		FinalizedRoot:  []byte{},
		FinalizedEpoch: 0,
		HeadRoot:       []byte{},
		HeadSlot:       0,
	}
	status = status

	_, err = stream.Write([]byte("Hello"))
	if err != nil {
		return nil, fmt.Errorf("write hello: %w", err)
	}

	resp, err := io.ReadAll(stream)
	if err != nil {
		return nil, fmt.Errorf("read status: %w", err)
	}

	fmt.Printf("%x\n", resp)

	return &Node{
		Client: common.Client{
			Name:     "",
			UserData: "",
			Version:  "",
			Build:    "",
			OS:       0,
			Arch:     0,
			Language: "",
		},
		Node:      &enode.Node{},
		Error:     "error",
		Direction: common.DirectionDial,
	}, nil
}
