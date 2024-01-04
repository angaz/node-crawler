package migrations

import (
	"context"
	"fmt"
	"time"

	"log/slog"

	"github.com/ethereum/node-crawler/pkg/networks"
	"github.com/jackc/pgx/v5"
)

func lastEphemeryRelease(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var maxTimestamp time.Time

	rows, err := tx.Query(
		ctx,
		`
			SELECT
				timestamp
			FROM network.ephemery_releases
			ORDER BY
				timestamp DESC
			LIMIT 1
		`,
	)
	if err != nil {
		return maxTimestamp, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&maxTimestamp)
	}

	return maxTimestamp, nil
}

func insertEphemeryReleases(ctx context.Context, tx pgx.Tx, networks []networks.EphemeryNetwork) error {
	stmt, err := tx.Prepare(
		ctx,
		"insert_ephemery_release",
		`
			INSERT INTO network.ephemery_releases (
				timestamp,
				network_id,
				network_name
			) VALUES (
				$1,
				$2,
				$3
			)
		`,
	)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}

	for _, network := range networks {
		_, err := tx.Exec(
			ctx,
			stmt.Name,

			network.PublishedAt,
			network.NetworkID,
			network.Name,
		)
		if err != nil {
			return fmt.Errorf("exec: %w", err)
		}
	}

	return nil
}

func insertForks(ctx context.Context, tx pgx.Tx, forks []networks.Fork) error {
	stmt, err := tx.Prepare(
		ctx,
		"insert_fork",
		`
			INSERT INTO network.forks (
				network_id,
				block_time,
				fork_id,
				previous_fork_id,
				fork_name,
				network_name
			) VALUES (
				$1,
				$2,
				$3,
				$4,
				$5,
				$6
			)
			ON CONFLICT (network_id, fork_id) DO NOTHING
		`,
	)
	if err != nil {
		return fmt.Errorf("prepare insert_fork: %w", err)
	}

	for _, fork := range forks {
		_, err := tx.Exec(
			ctx,
			stmt.Name,

			fork.NetworkID,
			fork.BlockTime,
			fork.ForkID,
			fork.PreviousForkID,
			fork.ForkName,
			fork.NetworkName,
		)
		if err != nil {
			return fmt.Errorf("exec: %w", err)
		}
	}

	return nil
}

func InsertNewEphemeryNetworks(ctx context.Context, tx pgx.Tx, githubToken string) error {
	lastEphemery, err := lastEphemeryRelease(ctx, tx)
	if err != nil {
		return fmt.Errorf("query last ephemery: %w", err)
	}

	startEphemery := time.Now()

	ephemeryNetworks, err := networks.GetEphemeryNetworks(githubToken, lastEphemery)
	if err != nil {
		return fmt.Errorf("get ephemerey networks: %w", err)
	}

	slog.Info("fetch ephemery networks done", "duration", time.Since(startEphemery))

	forks := make([]networks.Fork, 0, len(ephemeryNetworks))

	for _, network := range ephemeryNetworks {
		forks = append(forks, network.Forks...)
	}

	err = insertForks(ctx, tx, forks)
	if err != nil {
		return fmt.Errorf("insert forks: %w", err)
	}

	err = insertEphemeryReleases(ctx, tx, ephemeryNetworks)
	if err != nil {
		return fmt.Errorf("insert ephemery release: %w", err)
	}

	return nil
}

func InsertNetworks(ctx context.Context, tx pgx.Tx, githubToken string) error {
	err := insertForks(ctx, tx, networks.EthereumNetworks())
	if err != nil {
		return fmt.Errorf("insert forks: %w", err)
	}

	err = InsertNewEphemeryNetworks(ctx, tx, githubToken)
	if err != nil {
		return fmt.Errorf("insert ephemery networks: %w", err)
	}

	return nil
}
