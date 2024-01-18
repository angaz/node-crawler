package networks

import (
	"encoding/binary"
	"hash/crc32"
	"math/big"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/params"
)

type Fork struct {
	NetworkID      uint64
	BlockTime      uint64
	ForkID         uint32
	PreviousForkID *uint32
	ForkName       string
	NetworkName    string
}

var ethereumForkNames = []string{
	"Homestead",
	"DAO Fork",
	"Tangerine Whistle (EIP 150)",
	"Spurious Dragon/1 (EIP 155)",
	"Spurious Dragon/2 (EIP 158)",
	"Byzantium",
	"Constantinople",
	"Petersburg",
	"Istanbul",
	"Muir Glacier",
	"Berlin",
	"London",
	"Arrow Glacier",
	"Gray Glacier",
	"Merge",
	"Shanghai",
	"Cancun",
	"Prague",
	"Verkle",
}

func uint64ptr(i *big.Int) *uint64 {
	if i == nil {
		return nil
	}

	ui64 := i.Uint64()
	return &ui64
}

func chainForks(config *params.ChainConfig) ([]*uint64, []*uint64) {
	blocks := []*uint64{
		uint64ptr(config.HomesteadBlock),
		uint64ptr(config.DAOForkBlock),
		uint64ptr(config.EIP150Block),
		uint64ptr(config.EIP155Block),
		uint64ptr(config.EIP158Block),
		uint64ptr(config.ByzantiumBlock),
		uint64ptr(config.ConstantinopleBlock),
		uint64ptr(config.PetersburgBlock),
		uint64ptr(config.IstanbulBlock),
		uint64ptr(config.MuirGlacierBlock),
		uint64ptr(config.BerlinBlock),
		uint64ptr(config.LondonBlock),
		uint64ptr(config.ArrowGlacierBlock),
		uint64ptr(config.GrayGlacierBlock),
		uint64ptr(config.MergeNetsplitBlock),
	}

	times := []*uint64{
		config.ShanghaiTime,
		config.CancunTime,
		config.PragueTime,
		config.VerkleTime,
	}

	return blocks, times
}

func checksumUpdate(hash uint32, fork uint64) uint32 {
	var blob [8]byte
	binary.BigEndian.PutUint64(blob[:], fork)

	return crc32.Update(hash, crc32.IEEETable, blob[:])
}

func Forks(genesis *core.Genesis, networkName string) []Fork {
	networkID := genesis.Config.ChainID.Uint64()
	previousForkID := crc32.ChecksumIEEE(genesis.ToBlock().Hash().Bytes())

	out := make([]Fork, 0, 32)
	out = append(out, Fork{
		NetworkID:      networkID,
		ForkID:         previousForkID,
		BlockTime:      0,
		PreviousForkID: nil,
		ForkName:       "Genesis",
		NetworkName:    networkName,
	})

	blocks, times := chainForks(genesis.Config)

	var previousBlock uint64

	for i, block := range blocks {
		if block == nil {
			continue
		}

		if previousBlock == *block {
			continue
		}

		thisForkID := checksumUpdate(previousForkID, *block)

		pfid := previousForkID

		out = append(out, Fork{
			NetworkID:      genesis.Config.ChainID.Uint64(),
			ForkID:         thisForkID,
			BlockTime:      *block,
			PreviousForkID: &pfid,
			ForkName:       ethereumForkNames[i],
			NetworkName:    networkName,
		})

		previousBlock = *block
		previousForkID = thisForkID
	}

	var previousBlockTime uint64

	for i, blockTime := range times {
		if blockTime == nil {
			continue
		}

		if *blockTime <= genesis.Timestamp {
			continue
		}

		if previousBlockTime == *blockTime {
			continue
		}

		thisForkID := checksumUpdate(previousForkID, *blockTime)

		pfid := previousForkID

		out = append(out, Fork{
			NetworkID:      genesis.Config.ChainID.Uint64(),
			ForkID:         thisForkID,
			BlockTime:      *blockTime,
			PreviousForkID: &pfid,
			ForkName:       ethereumForkNames[i+len(blocks)],
			NetworkName:    networkName,
		})

		previousBlockTime = *blockTime
		previousForkID = thisForkID
	}

	return out
}
