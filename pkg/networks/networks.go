package networks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/ethereum/go-ethereum/params"
	"github.com/google/go-github/v57/github"
)

func ParseEthereumGenesisJSON(genesisReader io.Reader) (*params.ChainConfig, error) {
	var config params.ChainConfig

	decoder := json.NewDecoder(genesisReader)

	err := decoder.Decode(&config)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	return &config, nil
}

func ReadEthereumGenesisJSONFile(filename string) (*params.ChainConfig, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	config, err := ParseEthereumGenesisJSON(file)
	if err != nil {
		return nil, fmt.Errorf("parse genesis json: %w", err)
	}

	return config, nil
}

func GetEphemeryReleases(githubToken string, lastRelease string) ([]params.ChainConfig, error) {
	client := github.NewClient(nil)
	if githubToken != "" {
		client = client.WithAuthToken(githubToken)
	}

	releases, _, err := client.Repositories.ListReleases(
		context.Background(),
		"ephemery-testnet",
		"ephemery-genesis",
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("github list releases: %w", err)
	}

	for _, release := range releases {
		fmt.Println(release.GetName())
	}

	return nil, nil
}
