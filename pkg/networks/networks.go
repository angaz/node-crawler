package networks

import (
	"hash/crc32"

	"github.com/ethereum/go-ethereum/common"
)

type partialFork struct {
	BlockTime uint64
	ForkName  string
}

func concatSlices[T any](orig []T, more ...[]T) []T {
	concat := orig

	for _, s := range more {
		concat = append(concat, s...)
	}

	return concat
}

func ptr[T any](v T) *T {
	return &v
}

func makeForks(
	name string,
	networkID uint64,
	genesisHash common.Hash,
	forks []partialFork,
) []Fork {
	out := make([]Fork, 0, len(forks)+1)

	previousForkID := crc32.ChecksumIEEE(genesisHash.Bytes())

	out = append(out, Fork{
		NetworkID:      networkID,
		ForkID:         previousForkID,
		BlockTime:      0,
		PreviousForkID: nil,
		ForkName:       "Genesis",
		NetworkName:    name,
	})

	for _, fork := range forks {
		thisForkID := checksumUpdate(previousForkID, fork.BlockTime)

		out = append(out, Fork{
			NetworkID:      networkID,
			BlockTime:      fork.BlockTime,
			ForkID:         thisForkID,
			PreviousForkID: ptr(previousForkID),
			ForkName:       fork.ForkName,
			NetworkName:    name,
		})

		previousForkID = thisForkID
	}

	return out
}

var (
	mainnetForks = makeForks(
		"Mainnet",
		1,
		common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
		[]partialFork{
			{1150000, "Homestead"},
			{1920000, "DAO Fork"},
			{2463000, "Tangerine Whistle (EIP 150)"},
			{2675000, "Spurious Dragon/1 (EIP 155)"},
			{4370000, "Byzantium"},
			{7280000, "Constantinople"},
			{9069000, "Istanbul"},
			{9200000, "Muir Glacier"},
			{12244000, "Berlin"},
			{12965000, "London"},
			{13773000, "Arrow Glacier"},
			{15050000, "Gray Glacier"},
			{1681338455, "Shanghai"},
			{1710338135, "Cancun"},
		},
	)
	sepoliaForks = makeForks(
		"Sepolia",
		11155111,
		common.HexToHash("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9"),
		[]partialFork{
			{1735371, "Merge"},
			{1677557088, "Shanghai"},
			{1706655072, "Cancun"},
			{1741159776, "Prague"},
		},
	)
	holeskyForks = makeForks(
		"Holešky",
		17000,
		common.HexToHash("0xb5f7f912443c940f21fd611f12828d75b534364ed9e95ca4e307729a4661bde4"),
		[]partialFork{
			{1696000704, "Shanghai"},
			{1707305664, "Cancun"},
			{1740434112, "Prague"},
		},
	)
)

func EthereumNetworks() []Fork {
	return concatSlices(
		mainnetForks,
		sepoliaForks,
		holeskyForks,
	)
}
