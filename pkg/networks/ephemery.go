package networks

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
		if added < pageSize || resp.NextPage == 0 {
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

func downloadAsset(asset *github.ReleaseAsset) ([]byte, error) {
	resp, err := http.Get(*asset.BrowserDownloadURL)
	if err != nil {
		return nil, fmt.Errorf("http GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status not OK: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return body, nil
}

func releaseNetwork(release *github.RepositoryRelease) (*EphemeryNetwork, error) {
	var genesis *github.ReleaseAsset
	var config *github.ReleaseAsset

	for _, asset := range release.Assets {
		if asset.Name != nil && *asset.Name == "genesis.json" {
			genesis = asset
		}
		if asset.Name != nil && *asset.Name == "config.yaml" {
			config = asset
		}

	}

	if genesis == nil || config == nil {
		return nil, fmt.Errorf("genesis.json or config.yaml not found")
	}

	genesisBody, err := downloadAsset(genesis)

	execution, err := parseExecutionGenesisJSON(bytes.NewBuffer(genesisBody))
	if err != nil {
		return nil, fmt.Errorf("parse genesis json: %w", err)
	}

	consensusBody, err := downloadAsset(config)

	consensus, err := parseConsensusConfigYAML(consensusBody)
	if err != nil {
		return nil, fmt.Errorf("parse beacon yaml: %w", err)
	}

	name := ephemeryIterationName(release)

	return &EphemeryNetwork{
		Name:        name,
		NetworkID:   execution.Config.ChainID.Uint64(),
		PublishedAt: release.PublishedAt.Time,
		Forks: Forks(
			execution,
			consensus,
			name,
		),
	}, nil
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
