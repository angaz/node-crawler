package common

import (
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/p2p"
)

type ClientInfo struct {
	ClientType      string
	SoftwareVersion uint64
	Capabilities    []p2p.Cap
	NetworkID       uint64
	ForkID          forkid.ID
	Blockheight     string
	TotalDifficulty *big.Int
	HeadHash        common.Hash
}

func (c ClientInfo) String() string {
	return fmt.Sprintf(`Client Type: %s
Software Version: %d
Capabilities: %v
Network ID: %d
Fork ID: %v
Block Height: %s
Total Difficulty: %s
Head Hash: %s`,
		c.ClientType,
		c.SoftwareVersion,
		c.Capabilities,
		c.NetworkID,
		c.ForkID,
		c.Blockheight,
		c.TotalDifficulty.String(),
		hex.EncodeToString(c.HeadHash[:]),
	)
}
