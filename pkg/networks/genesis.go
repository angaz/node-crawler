package networks

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/ethereum/go-ethereum/core"
)

func parseEthereumGenesisJSON(genesisReader io.Reader) (*core.Genesis, error) {
	var genesis core.Genesis

	decoder := json.NewDecoder(genesisReader)

	err := decoder.Decode(&genesis)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	return &genesis, nil
}

func ReadEthereumGenesisJSONFile(filename string) (*core.Genesis, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	genesis, err := parseEthereumGenesisJSON(file)
	if err != nil {
		return nil, fmt.Errorf("parse genesis json: %w", err)
	}

	return genesis, nil
}
