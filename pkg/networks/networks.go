package networks

import (
	"hash/crc32"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
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

// Upgrade schedules are posted here: https://github.com/ethereum/pm
var (
	mainnetForks = makeForks(
		"Mainnet",
		1,
		params.MainnetGenesisHash,
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
			{1746612311, "Prague"},
			{1764798551, "Osaka"},
			{1765290071, "BPO1"},
			{1767747671, "BPO2"},
		},
	)
	sepoliaForks = makeForks(
		"Sepolia",
		11155111,
		params.SepoliaGenesisHash,
		[]partialFork{
			{1735371, "Merge"},
			{1677557088, "Shanghai"},
			{1706655072, "Cancun"},
			{1741159776, "Prague"},
			{1760427360, "Osaka"},
			{1761017184, "BPO1"},
			{1761607008, "BPO2"},
		},
	)
	holeskyForks = makeForks(
		"Holešky",
		17000,
		params.HoleskyGenesisHash,
		[]partialFork{
			{1696000704, "Shanghai"},
			{1707305664, "Cancun"},
			{1740434112, "Prague"},
			{1759308480, "Osaka"},
			{1759800000, "BPO1"},
			{1760389824, "BPO2"},
		},
	)
	hoodiForks = makeForks(
		"Hoodi",
		560048,
		params.HoodiGenesisHash,
		[]partialFork{
			{1742999832, "Prague"},
			{1761677592, "Osaka"},
			{1762365720, "BPO1"},
			{1762955544, "BPO2"},
		},
	)
)

func EthereumNetworks() []Fork {
	return concatSlices(
		mainnetForks,
		sepoliaForks,
		holeskyForks,
		hoodiForks,
	)
}
