package networks

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/go-github/v57/github"
)

type EphemeryNetwork struct {
	Name        string
	NetworkID   uint64
	PublishedAt time.Time
	Forks       []Fork
}

func getEphemeryReleases(githubToken string, lastRelease time.Time) ([]*github.RepositoryRelease, error) {
	var allReleases []*github.RepositoryRelease

	client := github.NewClient(nil)

	if githubToken != "" {
		client = client.WithAuthToken(githubToken)
	}

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

func ephemeryIterationName(release *github.RepositoryRelease) string {
	split := strings.Split(*release.Name, "-")

	if len(split) == 1 {
		return "Ephemery " + split[0]
	}

	return "Ephemery " + split[1]
}

func releaseNetwork(release *github.RepositoryRelease) (*EphemeryNetwork, error) {
	for _, asset := range release.Assets {
		if asset.Name == nil || *asset.Name != "genesis.json" {
			continue
		}

		resp, err := http.Get(*asset.BrowserDownloadURL)
		if err != nil {
			return nil, fmt.Errorf("download: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("download: status not OK: %d", resp.StatusCode)
		}

		genesis, err := parseEthereumGenesisJSON(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("parse genesis json: %w", err)
		}

		name := ephemeryIterationName(release)

		return &EphemeryNetwork{
			Name:        name,
			NetworkID:   genesis.Config.ChainID.Uint64(),
			PublishedAt: release.PublishedAt.Time,
			Forks: Forks(
				genesis,
				name,
			),
		}, nil
	}

	return nil, fmt.Errorf("no genesis.json release found")
}

func GetEphemeryNetworks(githubToken string, lastRelease time.Time) ([]EphemeryNetwork, error) {
	var networks []EphemeryNetwork

	releases, err := getEphemeryReleases(githubToken, lastRelease)
	if err != nil {
		return nil, fmt.Errorf("get releases: %w", err)
	}

	for _, release := range releases {
		network, err := releaseNetwork(release)
		if err != nil {
			return nil, fmt.Errorf("release forks: %s: %w", *release.Name, err)
		}

		networks = append(networks, *network)
	}

	return networks, nil
}
