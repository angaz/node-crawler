package networks

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/ethereum/go-ethereum/core"
	"github.com/prysmaticlabs/prysm/v4/config/params"
)

func parseExecutionGenesisJSON(genesisReader io.Reader) (*core.Genesis, error) {
	var genesis core.Genesis

	decoder := json.NewDecoder(genesisReader)

	err := decoder.Decode(&genesis)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	return &genesis, nil
}

func ReadExecutionGenesisJSONFile(filename string) (*core.Genesis, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	genesis, err := parseExecutionGenesisJSON(file)
	if err != nil {
		return nil, fmt.Errorf("parse genesis json: %w", err)
	}

	return genesis, nil
}

func parseConsensusConfigYAML(config []byte) (*params.BeaconChainConfig, error) {
	consensus, err := params.UnmarshalConfig(config, nil)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return consensus, nil
}
