package common

import (
	"encoding/binary"
	"hash/crc32"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

var (
	bscGenesisHash       = common.HexToHash("0x0d21840abff46b96c84b2ac9e10e4f5cdaeb5693cb665db62a2f3b02d2d57b5b")
	bscChainID     int64 = 56
	bscForks             = []*uint64{
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(0)),
		uint64ptr(big.NewInt(5184000)),
		uint64ptr(big.NewInt(13082000)),
		uint64ptr(big.NewInt(18907621)),
		uint64ptr(big.NewInt(21962149)),
		uint64ptr(big.NewInt(22107423)),
		uint64ptr(big.NewInt(23846001)),
		uint64ptr(big.NewInt(27281024)),
		uint64ptr(big.NewInt(29020050)),
		uint64ptr(big.NewInt(30720096)),
		uint64ptr(big.NewInt(31302048)),
		uint64ptr(big.NewInt(31302048)),
		uint64ptr(big.NewInt(31302048)),
	}
)

type ForkMaps struct {
	Hash      map[uint32]string
	BlockTime map[uint64]string
	NextFork  *uint64
}

var Chains = map[int64]*params.ChainConfig{
	params.MainnetChainConfig.ChainID.Int64(): params.MainnetChainConfig,
	params.GoerliChainConfig.ChainID.Int64():  params.GoerliChainConfig,
	params.SepoliaChainConfig.ChainID.Int64(): params.SepoliaChainConfig,
	params.HoleskyChainConfig.ChainID.Int64(): params.HoleskyChainConfig,
}

var Forks = map[int64]ForkMaps{
	params.MainnetChainConfig.ChainID.Int64(): createEthereumForkMap(
		core.DefaultGenesisBlock().ToBlock(),
		params.MainnetChainConfig,
	),
	params.GoerliChainConfig.ChainID.Int64(): createEthereumForkMap(
		core.DefaultGoerliGenesisBlock().ToBlock(),
		params.GoerliChainConfig,
	),
	params.SepoliaChainConfig.ChainID.Int64(): createEthereumForkMap(
		core.DefaultSepoliaGenesisBlock().ToBlock(),
		params.SepoliaChainConfig,
	),
	params.HoleskyChainConfig.ChainID.Int64(): createEthereumForkMap(
		core.DefaultHoleskyGenesisBlock().ToBlock(),
		params.HoleskyChainConfig,
	),
	bscChainID: createForkMap(
		bscGenesisHash.Bytes(),
		0,
		bscForkNames,
		bscForks,
		[]*uint64{},
		nil,
	),
}

var (
	ForkNameHomeStead      = "Homestead"
	ForkNameDAO            = "DAO Fork"
	ForkNameEIP150         = "Tangerine Whistle (EIP 150)"
	ForkNameEIP155         = "Spurious Dragon/1 (EIP 155)"
	ForkNameEIP158         = "Spurious Dragon/2 (EIP 158)"
	ForkNameByzantium      = "Byzantium"
	ForkNameConstantinople = "Constantinople"
	ForkNamePetersburg     = "Petersburg"
	ForkNameIstanbul       = "Istanbul"
	ForkNameMuirGlacier    = "Muir Glacier"
	ForkNameBerlin         = "Berlin"
	ForkNameLondon         = "London"
	ForkNameArrowGlacier   = "Arrow Glacier"
	ForkNameGrayGlacier    = "Gray Glacier"
	ForkNameMerge          = "Merge"
	ForkNameShanghai       = "Shanghai"
	ForkNameCancun         = "Cancun"
	ForkNamePrague         = "Prague"
	ForkNameVerkle         = "Verkle"
	ForkNameRamanujan      = "Ramanujan"
	ForkNameNiels          = "Niels"
	ForkNameMirrorSync     = "MirrorSync"
	ForkNameBruno          = "Bruno"
	ForkNameEuler          = "Euler"
	ForkNameGibbs          = "Gibbs"
	ForkNameNano           = "Nano"
	ForkNameMoran          = "Moran"
	ForkNamePlanck         = "Planck"
	ForkNameLuban          = "Luban"
	ForkNamePlato          = "Plato"
	ForkNameHertz          = "Hertz"
)

var bscForkNames = []string{
	ForkNameHomeStead,
	ForkNameEIP150,
	ForkNameEIP155,
	ForkNameEIP158,
	ForkNameByzantium,
	ForkNameConstantinople,
	ForkNamePetersburg,
	ForkNameIstanbul,
	ForkNameMuirGlacier,
	ForkNameRamanujan,
	ForkNameNiels,
	ForkNameMirrorSync,
	ForkNameBruno,
	ForkNameEuler,
	ForkNameNano,
	ForkNameMoran,
	ForkNameGibbs,
	ForkNamePlanck,
	ForkNameLuban,
	ForkNamePlato,
	ForkNameBerlin,
	ForkNameLondon,
	ForkNameHertz,
}

var EthereumForkNames = []string{
	ForkNameHomeStead,
	ForkNameDAO,
	ForkNameEIP150,
	ForkNameEIP155,
	ForkNameEIP158,
	ForkNameByzantium,
	ForkNameConstantinople,
	ForkNamePetersburg,
	ForkNameIstanbul,
	ForkNameMuirGlacier,
	ForkNameBerlin,
	ForkNameLondon,
	ForkNameArrowGlacier,
	ForkNameGrayGlacier,
	ForkNameMerge,
	ForkNameShanghai,
	ForkNameCancun,
	ForkNamePrague,
	ForkNameVerkle,
}

func checksumUpdate(hash uint32, fork uint64) uint32 {
	var blob [8]byte
	binary.BigEndian.PutUint64(blob[:], fork)

	return crc32.Update(hash, crc32.IEEETable, blob[:])
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

func createForkMap(
	genesis []byte,
	genesisTime uint64,
	forkNames []string,
	blocks []*uint64,
	times []*uint64,
	nextFork *uint64,
) ForkMaps {
	currentHash := crc32.ChecksumIEEE(genesis)

	out := ForkMaps{
		Hash: map[uint32]string{
			currentHash: "Genesis",
		},
		BlockTime: map[uint64]string{},
		NextFork:  nextFork,
	}

	var previousBlock uint64 = 0

	for i, block := range blocks {
		// Deduplicate blocks in the same fork
		if block != nil && previousBlock != *block {
			currentHash = checksumUpdate(currentHash, *block)

			name := forkNames[i]
			out.Hash[currentHash] = name
			out.BlockTime[*block] = name

			previousBlock = *block
		}
	}

	var previousBlockTime uint64 = 0

	for i, blockTime := range times {
		// Deduplicate blocks in the same fork
		if blockTime != nil && previousBlockTime != *blockTime {
			currentHash = checksumUpdate(currentHash, *blockTime)

			name := forkNames[i+len(blocks)]
			out.Hash[currentHash] = name
			out.BlockTime[*blockTime] = name

			previousBlockTime = *blockTime
		}
	}

	return out
}

func createEthereumForkMap(genesis *types.Block, config *params.ChainConfig) ForkMaps {
	blocks, times := chainForks(config)
	return createForkMap(
		genesis.Hash().Bytes(),
		genesis.Time(),
		EthereumForkNames,
		blocks,
		times,
		config.CancunTime,
	)
}
