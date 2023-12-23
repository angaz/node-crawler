package networks

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/google/go-github/v57/github"
)

type EphemeryNetwork struct {
	Name        string
	PublishedAt time.Time
	Forks       []Fork
}

func getEphemeryReleases(lastRelease time.Time) ([]*github.RepositoryRelease, error) {
	var allReleases []*github.RepositoryRelease

	client := github.NewClient(nil)

	pageNum := 1
	pageSize := 10

	for {
		releases, resp, err := client.Repositories.ListReleases(
			context.Background(),
			"ephemery-testnet",
			"ephemery-genesis",
			&github.ListOptions{
				Page:    pageNum,
				PerPage: pageSize,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("github list releases: %w", err)
		}

		added := 0

		for _, release := range releases {
			if release.PublishedAt.After(lastRelease) {
				allReleases = append(allReleases, release)
				added++
			}
		}

		// reached the end
		if added < pageSize {
			return allReleases, nil
		}

		pageNum = resp.NextPage
	}
}

func ephemeryIteration(genesis *core.Genesis) uint64 {
	return genesis.Config.ChainID.Uint64() - 39438000
}

func releaseNetwork(release *github.RepositoryRelease) (*EphemeryNetwork, error) {
	for _, asset := range release.Assets {
		if asset.Name == nil || *asset.Name != "genesis.json" {
			continue
		}

		resp, err := http.Get(*asset.URL)
		if err != nil {
			return nil, fmt.Errorf("download: %w", err)
		}
		defer resp.Body.Close()

		genesis, err := parseEthereumGenesisJSON(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("parse genesis json: %w", err)
		}

		name := fmt.Sprintf("Ephemery %d", ephemeryIteration(genesis))

		return &EphemeryNetwork{
			Name:        name,
			PublishedAt: release.PublishedAt.Time,
			Forks: Forks(
				genesis,
				name,
			),
		}, nil
	}

	return nil, fmt.Errorf("no genesis.json release found")
}

func GetEphemeryNetworks(lastRelease time.Time) ([]EphemeryNetwork, error) {
	var networks []EphemeryNetwork

	releases, err := getEphemeryReleases(lastRelease)
	if err != nil {
		return nil, fmt.Errorf("get releases: %w", err)
	}

	for _, release := range releases {
		network, err := releaseNetwork(release)
		if err != nil {
			return nil, fmt.Errorf("release forks: %w", err)
		}

		networks = append(networks, *network)
	}

	return networks, nil
}
