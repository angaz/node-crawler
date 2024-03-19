package networks

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/big"

	"github.com/ethereum/go-ethereum/core"
	gethparams "github.com/ethereum/go-ethereum/params"
	prysmparams "github.com/prysmaticlabs/prysm/v4/config/params"
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

func chainForks(executionConfig *gethparams.ChainConfig, beaconConfig *prysmparams.BeaconChainConfig) ([]*uint64, []*uint64, []*uint64) {
	blocks := []*uint64{
		uint64ptr(executionConfig.HomesteadBlock),
		uint64ptr(executionConfig.DAOForkBlock),
		uint64ptr(executionConfig.EIP150Block),
		uint64ptr(executionConfig.EIP155Block),
		uint64ptr(executionConfig.EIP158Block),
		uint64ptr(executionConfig.ByzantiumBlock),
		uint64ptr(executionConfig.ConstantinopleBlock),
		uint64ptr(executionConfig.PetersburgBlock),
		uint64ptr(executionConfig.IstanbulBlock),
		uint64ptr(executionConfig.MuirGlacierBlock),
		uint64ptr(executionConfig.BerlinBlock),
		uint64ptr(executionConfig.LondonBlock),
		uint64ptr(executionConfig.ArrowGlacierBlock),
		uint64ptr(executionConfig.GrayGlacierBlock),
		uint64ptr(executionConfig.MergeNetsplitBlock),
	}

	times := []*uint64{
		executionConfig.ShanghaiTime,
		executionConfig.CancunTime,
		executionConfig.PragueTime,
		executionConfig.VerkleTime,
	}

	epochs := []*uint64{
		(*uint64)(&beaconConfig.AltairForkEpoch),
		(*uint64)(&beaconConfig.BellatrixForkEpoch),
		(*uint64)(&beaconConfig.CapellaForkEpoch),
		(*uint64)(&beaconConfig.DenebForkEpoch),
	}

	fmt.Printf("%#v\n", epochs)

	return blocks, times, epochs
}

func checksumUpdate(hash uint32, fork uint64) uint32 {
	var blob [8]byte
	binary.BigEndian.PutUint64(blob[:], fork)

	return crc32.Update(hash, crc32.IEEETable, blob[:])
}

func Forks(execution *core.Genesis, consensus *prysmparams.BeaconChainConfig, networkName string) []Fork {
	networkID := execution.Config.ChainID.Uint64()
	previousForkID := crc32.ChecksumIEEE(execution.ToBlock().Hash().Bytes())

	out := make([]Fork, 0, 32)
	out = append(out, Fork{
		NetworkID:      networkID,
		ForkID:         previousForkID,
		BlockTime:      0,
		PreviousForkID: nil,
		ForkName:       "Genesis",
		NetworkName:    networkName,
	})

	blocks, times, _ := chainForks(execution.Config, consensus)

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
			NetworkID:      execution.Config.ChainID.Uint64(),
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

		if *blockTime <= execution.Timestamp {
			continue
		}

		if previousBlockTime == *blockTime {
			continue
		}

		thisForkID := checksumUpdate(previousForkID, *blockTime)

		pfid := previousForkID

		out = append(out, Fork{
			NetworkID:      execution.Config.ChainID.Uint64(),
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
