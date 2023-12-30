// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package common

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	pb "github.com/prysmaticlabs/prysm/v4/proto/prysm/v1alpha1"

	"github.com/ethereum/go-ethereum/rlp"
)

type NodeType int

const (
	NodeTypeUnknown   NodeType = 0
	NodeTypeExecution NodeType = 1
	NodeTypeConsensus NodeType = 2
)

var (
	nodeTypeStrings = [...]string{
		NodeTypeUnknown:   "Unknown",
		NodeTypeExecution: "Execution",
		NodeTypeConsensus: "Consensus",
	}
	stringToNodeTypes = map[string]NodeType{
		"Unknown":   NodeTypeUnknown,
		"Execution": NodeTypeExecution,
		"Consensus": NodeTypeConsensus,
	}
)

func (n NodeType) String() string {
	if int(n) >= len(nodeTypeStrings) {
		return "Invalid type"
	}

	return nodeTypeStrings[n]
}

func ParseNodeType(s string) NodeType {
	nodeType, ok := stringToNodeTypes[s]
	if !ok {
		return NodeTypeUnknown
	}

	return nodeType
}

func (dst *NodeType) Scan(src any) error {
	if src == nil {
		return nil
	}

	switch src := src.(type) {
	case string:
		*dst = ParseNodeType(src)

		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

func IsEnodeV4(source string) bool {
	return strings.HasPrefix(source, "enode://")
}

// ParseNode parses a node record and verifies its signature.
func ParseNode(source string) (*enode.Node, error) {
	if IsEnodeV4(source) {
		return enode.ParseV4(source)
	}

	r, err := parseRecord(source)
	if err != nil {
		return nil, err
	}

	return enode.New(enode.ValidSchemes, r)
}

func EncodeENR(r *enr.Record) []byte {
	// Always succeeds because record is valid.
	nodeRecord, _ := rlp.EncodeToBytes(r)

	return nodeRecord
}

func LoadENR(b []byte) (*enr.Record, error) {
	var record enr.Record

	err := rlp.DecodeBytes(b, &record)
	if err != nil {
		return nil, fmt.Errorf("decode bytes failed: %w", err)
	}

	return &record, nil
}

func RecordPubKey(r *enr.Record) (*ecdsa.PublicKey, error) {
	var pubkey enode.Secp256k1

	err := r.Load(&pubkey)
	if err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}

	return (*ecdsa.PublicKey)(&pubkey), nil
}

func PubkeyBytes(pubkey *ecdsa.PublicKey) []byte {
	return crypto.FromECDSAPub(pubkey)[1:]
}

var IDv4 = string(enr.IDv4)

type ETH struct {
	ForkID     [4]byte
	NextForkID uint64
}

func (e ETH) ENRKey() string { return "eth" }

type ETH2 struct {
	*pb.ENRForkID
}

func (e *ETH2) ENRKey() string { return "eth2" }

func (e *ETH2) DecodeRLP(s *rlp.Stream) error {
	var sszEncodedForkEntry []byte
	var forkEntry pb.ENRForkID

	err := s.Decode(&sszEncodedForkEntry)
	if err != nil {
		return fmt.Errorf("rlpdecode: %w", err)
	}

	err = forkEntry.UnmarshalSSZ(sszEncodedForkEntry)
	if err != nil {
		return fmt.Errorf("unmarshalssz: %w", err)
	}

	e.ENRForkID = &forkEntry

	return nil
}

// Returns if this Record belongs to en EL (Execution Layer) Client, if it
// contains the `eth` key, or CL (Consensus Layer) Client, if it contains the
// `eth2` key, or Unknown, if it contains neither.
func ENRNodeType(r *enr.Record) NodeType {
	var (
		eth  ETH
		eth2 ETH2
	)

	// Record is Enode
	if r.IdentityScheme() == "" {
		return NodeTypeExecution
	}

	if err := r.Load(&eth); err == nil {
		return NodeTypeExecution
	}

	if err := r.Load(&eth2); err == nil {
		return NodeTypeConsensus
	}

	return NodeTypeUnknown
}

// Finds the best Record.
// The best record is the one with the highest sequence number.
// If the sequence numbers are the same, then B is returned.
//
// Since enode records don't have a sequence number, ENRs will naturally be
// preferred.
func BestRecord(a, b *enr.Record) *enr.Record {
	if a == nil {
		return b
	}

	if b == nil {
		return a
	}

	if a.Seq() > b.Seq() {
		return a
	}

	return b
}

func ENRString(r *enr.Record) string {
	// Always succeeds because record is valid.
	enc, _ := rlp.EncodeToBytes(r)
	b64 := base64.RawURLEncoding.EncodeToString(enc)

	return "enr:" + b64
}

func RecordIP(r *enr.Record) net.IP {
	var (
		ip4 enr.IPv4
		ip6 enr.IPv6
	)

	if r.Load(&ip4) == nil {
		return net.IP(ip4)
	}

	if r.Load(&ip6) == nil {
		return net.IP(ip6)
	}

	return nil
}

func RecordToEnodeV4(r *enr.Record) *enode.Node {
	ip := RecordIP(r)

	var tcp enr.TCP
	_ = r.Load(&tcp)

	var udp enr.UDP
	_ = r.Load(&udp)

	var pubkey enode.Secp256k1

	_ = r.Load(&pubkey)

	pkey := ecdsa.PublicKey(pubkey)

	return enode.NewV4(&pkey, ip, int(tcp), int(udp))
}

func IsEnode(r *enr.Record) bool {
	return r.IdentityScheme() == ""

}

func RecordToEnode(r *enr.Record) (*enode.Node, error) {
	if IsEnode(r) {
		return RecordToEnodeV4(r), nil
	}

	return enode.New(enode.ValidSchemes, r)
}

func EnodeString(r *enr.Record) string {
	node, err := RecordToEnode(r)
	if err != nil {
		log.Error("enode to string failed", "err", err)
		return ""
	}

	return node.URLv4()
}

// parseRecord parses a node record from hex, base64, or raw binary input.
func parseRecord(source string) (*enr.Record, error) {
	bin := []byte(source)
	if d, ok := decodeRecordHex(bytes.TrimSpace(bin)); ok {
		bin = d
	} else if d, ok := decodeRecordBase64(bytes.TrimSpace(bin)); ok {
		bin = d
	}
	var r enr.Record
	err := rlp.DecodeBytes(bin, &r)
	return &r, err
}

func decodeRecordHex(b []byte) ([]byte, bool) {
	if bytes.HasPrefix(b, []byte("0x")) {
		b = b[2:]
	}
	dec := make([]byte, hex.DecodedLen(len(b)))
	_, err := hex.Decode(dec, b)
	return dec, err == nil
}

func decodeRecordBase64(b []byte) ([]byte, bool) {
	if bytes.HasPrefix(b, []byte("enr:")) {
		b = b[4:]
	}
	dec := make([]byte, base64.RawURLEncoding.DecodedLen(len(b)))
	n, err := base64.RawURLEncoding.Decode(dec, b)
	return dec[:n], err == nil
}

// attrFormatters contains formatting functions for well-known ENR keys.
var attrFormatters = map[string]func(rlp.RawValue) (string, bool){
	"id":   formatAttrString,
	"ip":   formatAttrIP,
	"ip6":  formatAttrIP,
	"tcp":  formatAttrUint,
	"tcp6": formatAttrUint,
	"udp":  formatAttrUint,
	"udp6": formatAttrUint,
}

func formatAttrRaw(v rlp.RawValue) (string, bool) {
	s := hex.EncodeToString(v)
	return s, true
}

func formatAttrString(v rlp.RawValue) (string, bool) {
	content, _, err := rlp.SplitString(v)
	return strconv.Quote(string(content)), err == nil
}

func formatAttrIP(v rlp.RawValue) (string, bool) {
	content, _, err := rlp.SplitString(v)
	if err != nil || len(content) != 4 && len(content) != 6 {
		return "", false
	}
	return net.IP(content).String(), true
}

func formatAttrUint(v rlp.RawValue) (string, bool) {
	var x uint64
	if err := rlp.DecodeBytes(v, &x); err != nil {
		return "", false
	}
	return strconv.FormatUint(x, 10), true
}
