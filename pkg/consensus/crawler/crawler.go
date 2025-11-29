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
	ssz "github.com/ferranbt/fastssz"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jackc/pgx/v5"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
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

func (*Crawler) Close() {}

func (c *Crawler) StartDeamon(ctx context.Context, workers int) error {
	for range workers {
		c.wg.Add(1)
		c.crawler(ctx)
	}

	return nil
}

func (c *Crawler) randomHost() *consensusHost {
	idx := rand.Intn(len(c.hosts))

	return c.hosts[idx]
}

func (c *Crawler) crawlNode(ctx context.Context, _ pgx.Tx, node *enode.Node) error {
	consensusNode, err := GetClientInfo(ctx, c.randomHost().host, node)
	if err != nil {
		return fmt.Errorf("get client info: %w", err)
	}

	fmt.Printf("%#v\n", consensusNode)
	fmt.Printf("%#v\n", *consensusNode.Status)

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

	node := enode.MustParse("enr:-LS4QK9FHJBKZfrL3eSAcH_FOslKHBSaAm4SJPgeB1mXCE0VS-M-o2Nz4fS51lLqyX7mNAH8STW4Lyq2xKjBwg_BWnOCAS6HYXR0bmV0c4gAgAEAAAAAAIRldGgykGmuDpkFAXAA__________-CaWSCdjSCaXCErK4j-YlzZWNwMjU2azGhAugUbn6-41fo3j2-eRMFoDdmwEMXwHfQMapY4a1EbPnDg3RjcIIjKIN1ZHCCIyg")

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
		libp2p.DefaultMuxers,
		libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connManager),
	)
	if err != nil {
		return nil, err
	}

	host.SetStreamHandler("/eth2/beacon_chain/req/status/1/ssz_snappy", func(s network.Stream) {
		fmt.Println("We got a /eth2/beacon_chain/req/status/1/ssz_snappy")

		s.Close()
	})
	host.SetStreamHandler("/eth2/beacon_chain/req/ping/1/ssz_snappy", func(s network.Stream) {
		fmt.Println("We got a /eth2/beacon_chain/req/ping/1/ssz_snappy")

		s.Close()
	})

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
	Status    *consensustypes.Status
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

	status, err := GetStatus(ctx, host, info.ID)
	if err != nil {
		return nil, fmt.Errorf("get status: %w", err)
	}

	ua, err := GetPeerInfo(host, info.ID)
	if err != nil {
		return nil, fmt.Errorf("get peer info: %w", err)
	}

	fmt.Printf("addr: %s\nUA: %s\n", info.Addrs[0].String(), ua)

	return &Node{
		Client:    common.ParseClientID(&ua).Deref(),
		Node:      node,
		Error:     "",
		Direction: common.DirectionDial,
		Status:    status,
	}, nil
}

func GetStatus(ctx context.Context, host host.Host, peerID peer.ID) (*consensustypes.Status, error) {
	stream, err := host.NewStream(ctx, peerID, "/eth2/beacon_chain/req/status/1/ssz_snappy")
	if err != nil {
		return nil, fmt.Errorf("new stream: %w", err)
	}

	status := consensustypes.Status{
		ForkDigest:     []byte{0, 0, 0, 0},
		FinalizedRoot:  []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		FinalizedEpoch: 0,
		HeadRoot:       []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		HeadSlot:       0,
	}

	err = WriteSSZSnappy(stream, &status)
	if err != nil {
		return nil, fmt.Errorf("write status: %w", err)
	}

	err = stream.CloseWrite()
	if err != nil {
		return nil, fmt.Errorf("close write: %w", err)
	}

	respStatus := new(consensustypes.Status)
	err = ReadSnappySSZ(stream, respStatus)
	if err != nil {
		return nil, fmt.Errorf("read status: %w", err)
	}

	return respStatus, nil
}

func GetPeerInfo(
	host host.Host,
	id peer.ID,
) (string, error) {
	ps := host.Peerstore()

	ua, err := ps.Get(id, "AgentVersion")
	if err != nil {
		return "", fmt.Errorf("get agent version: %w", err)
	}

	return ua.(string), nil
}

func WriteSSZSnappy(w io.Writer, src ssz.Marshaler) error {
	encoded, err := src.MarshalSSZ()
	if err != nil {
		return fmt.Errorf("marshal ssz: %w", err)
	}

	_, err = w.Write(proto.EncodeVarint(uint64(len(encoded))))
	if err != nil {
		return fmt.Errorf("write size: %w", err)
	}

	snappyWriter := snappy.NewBufferedWriter(w)
	_, err = snappyWriter.Write(encoded)
	if err != nil {
		return fmt.Errorf("write snappy: %w", err)
	}

	err = snappyWriter.Close()
	if err != nil {
		return fmt.Errorf("writer close: %w", err)
	}

	return nil
}

const maxVarintLength = 10

var errExcessMaxLength = errors.Errorf("provided header exceeds the max varint length of %d bytes", maxVarintLength)

func readVarint(r io.Reader) (uint64, error) {
	varintBuf := make([]byte, 0, maxVarintLength)

	for range maxVarintLength {
		oneByte := [1]byte{0}

		n, err := r.Read(oneByte[:])
		if err != nil {
			return 0, err
		}

		if n != 1 {
			return 0, errors.New("did not read a byte from stream")
		}

		varintBuf = append(varintBuf, oneByte[0])

		// If most significant bit is not set, we have reached the end of the Varint.
		if oneByte[0]&0x80 == 0 {
			varint, n := proto.DecodeVarint(varintBuf)
			if n != len(varintBuf) {
				return 0, errors.New("varint did not decode entire byte slice")
			}

			return varint, nil
		}
	}

	return 0, errExcessMaxLength
}

const (
	responseCodeSuccess             byte = 0x00
	responseCodeInvalidRequest      byte = 0x01
	responseCodeServerError         byte = 0x02
	responseCodeResourceUnavailable byte = 0x03
)

func readStatusCode(r io.Reader) (byte, string, error) {
	oneByte := [1]byte{0}

	_, err := r.Read(oneByte[:])
	if err != nil {
		return 0, "", fmt.Errorf("read: %w", err)
	}

	statusCode := oneByte[0]

	if statusCode == responseCodeSuccess {
		return responseCodeSuccess, "", nil
	}

	errMsg, err := io.ReadAll(r)
	if err != nil {
		return statusCode, "", fmt.Errorf("read error: %w", err)
	}

	return statusCode, string(errMsg), nil
}

func ReadSnappySSZ(r io.Reader, target ssz.Unmarshaler) error {
	statusCode, errMsg, err := readStatusCode(r)
	if err != nil {
		return fmt.Errorf("read status: %w", err)
	}

	if statusCode != responseCodeSuccess {
		return fmt.Errorf("error from client: %d: %s", statusCode, errMsg)
	}

	size, err := readVarint(r)
	if err != nil {
		return fmt.Errorf("read varint: %w", err)
	}

	snappyReader := snappy.NewReader(r)

	uncompressed := make([]byte, size)
	_, err = io.ReadFull(snappyReader, uncompressed)
	if err != nil {
		return fmt.Errorf("read snappy: %w", err)
	}

	err = target.UnmarshalSSZ(uncompressed)
	if err != nil {
		return fmt.Errorf("unmarshal ssz: %w", err)
	}

	return nil
}
