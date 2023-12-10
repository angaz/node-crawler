package p2p

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/rlpx"
	"github.com/ethereum/node-crawler/pkg/version"
)

var (
	clientName = version.ClientName("NodeCrawler")
)

func Accept(pk *ecdsa.PrivateKey, fd net.Conn) (*ecdsa.PublicKey, *Conn, error) {
	conn := new(Conn)
	conn.ourKey = pk

	conn.Conn = rlpx.NewConn(fd, nil)

	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, nil, fmt.Errorf("cannot set conn deadline: %w", err)
	}

	pubKey, err := conn.Handshake(pk)
	if err != nil {
		return nil, nil, fmt.Errorf("handshake failed: %w", err)
	}

	return pubKey, conn, nil
}

// Dial attempts to Dial the given node and perform a handshake.
func Dial(pk *ecdsa.PrivateKey, n *enode.Node, timeout time.Duration) (*Conn, error) {
	var conn Conn
	conn.ourKey = pk

	fd, err := net.DialTimeout("tcp", fmt.Sprintf("[%s]:%d", n.IP(), n.TCP()), timeout)
	if err != nil {
		return nil, err
	}

	conn.Conn = rlpx.NewConn(fd, n.Pubkey())

	if err = conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, fmt.Errorf("cannot set conn deadline: %w", err)
	}

	_, err = conn.Handshake(pk)
	if err != nil {
		return nil, err
	}

	return &conn, nil
}

func (conn *Conn) writeHello() error {
	pub0 := crypto.FromECDSAPub(&conn.ourKey.PublicKey)[1:]

	h := &Hello{
		Name:    clientName,
		Version: 5,
		Caps: []p2p.Cap{
			{Name: "eth", Version: 66},
			{Name: "eth", Version: 67},
			{Name: "eth", Version: 68},
			{Name: "snap", Version: 1},
		},
		ListenPort: 0,
		ID:         pub0,
		Rest:       nil,
	}

	conn.ourHighestProtoVersion = 68
	conn.ourHighestSnapProtoVersion = 1

	return conn.Write(h)
}

func TranslateError(err error) (bool, string) {
	switch errStr := err.Error(); {
	case strings.Contains(errStr, "i/o timeout"):
		return true, "i/o timeout"
	case strings.Contains(errStr, "connection reset by peer"):
		return true, "connection reset by peer"
	case strings.Contains(errStr, "EOF"):
		return true, "EOF"
	case strings.Contains(errStr, "no route to host"):
		return true, "no route to host"
	case strings.Contains(errStr, "connection refused"):
		return true, "connection refused"
	case strings.Contains(errStr, "network is unreachable"):
		return true, "network is unreachable"
	case strings.Contains(errStr, "invalid message"):
		return true, "invalid message"
	case strings.Contains(errStr, "invalid public key"):
		return true, "invalid public key"
	case strings.Contains(errStr, "corrupt input"):
		return true, "corrupt input"
	case strings.Contains(errStr, "could not rlp decode message"):
		return true, "rlp decode"
	default:
		return false, errStr
	}
}
