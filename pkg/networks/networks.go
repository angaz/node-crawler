package networks

import "github.com/ethereum/go-ethereum/core"

func concatSlices[T any](orig []T, more ...[]T) []T {
	concat := orig

	for _, s := range more {
		concat = append(concat, s...)
	}

	return concat
}

func EthereumNetworks() []Fork {
	return concatSlices(
		Forks(core.DefaultGenesisBlock(), "Ethereum Mainnet"),
		Forks(core.DefaultGoerliGenesisBlock(), "Goerli"),
		Forks(core.DefaultHoleskyGenesisBlock(), "Holešky"),
		Forks(core.DefaultSepoliaGenesisBlock(), "Sepolia"),
	)
}
