package disc

import (
	"errors"
	"net"
	"net/netip"

	"github.com/ethereum/go-ethereum/p2p/discover"
)

// Stolen from https://github.com/ethereum/go-ethereum/blob/master/p2p/server.go

// sharedUDPConn implements a shared connection. Write sends messages to the underlying connection while read returns
// messages that were found unprocessable and sent to the unhandled channel by the primary listener.
type sharedUDPConn struct {
	*net.UDPConn
	unhandled chan discover.ReadPacket
}

// ReadFromUDP implements discover.UDPConn
func (s *sharedUDPConn) ReadFromUDP(b []byte) (n int, addr netip.AddrPort, err error) {
	packet, ok := <-s.unhandled
	if !ok {
		return 0, netip.AddrPort{}, errors.New("connection was closed")
	}

	l := len(packet.Data)

	if l > len(b) {
		l = len(b)
	}

	copy(b[:l], packet.Data[:l])

	return l, packet.Addr, nil
}

// Close implements discover.UDPConn
func (s *sharedUDPConn) Close() error {
	return nil
}
