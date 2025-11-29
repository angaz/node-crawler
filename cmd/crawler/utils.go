package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/angaz/sqlu"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/urfave/cli/v2"
)

func readGithubTokenFile(cCtx *cli.Context) (string, error) {
	filename := githubTokenFileFlag.Get(cCtx)

	if filename == "" {
		return "", nil
	}

	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}

	data, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("read: %w", err)
	}

	return strings.TrimSpace(string(data)), nil
}

func openSQLiteDB(cCtx *cli.Context, mode sqlu.Param) (*sql.DB, error) {
	crawlerDBName := crawlerDBFlag.Get(cCtx)

	if crawlerDBName == "" {
		return nil, nil
	}

	dsn := sqlu.ConnParams{
		Filename: crawlerDBName,
		Mode:     mode,
		Pragma:   []sqlu.Pragma{},
		// Pragma: []sqlu.Pragma{
		// 	sqlu.PragmaBusyTimeout(int64(busyTimeoutFlag.Get(cCtx))),
		// 	sqlu.PragmaJournalSizeLimit(128 * 1024 * 1024), // 128MiB
		// 	sqlu.PragmaAutoVacuumIncremental,
		// 	sqlu.PragmaJournalModeWAL,
		// 	sqlu.PragmaSynchronousNormal,
		// },
		Attach: []sqlu.AttachParams{
			{
				Filename: statsDBFlag.Get(cCtx),
				Database: "stats",
				Mode:     mode,
				Pragma:   []sqlu.Pragma{},
				// Pragma: []sqlu.Pragma{
				// 	sqlu.PragmaJournalSizeLimit(128 * 1024 * 1024), // 128MiB
				// 	sqlu.PragmaAutoVacuumIncremental,
				// 	sqlu.PragmaJournalModeWAL,
				// 	sqlu.PragmaSynchronousNormal,
				// },
			},
		},
	}

	db, err := sql.Open("sqlite", dsn.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	return db, nil
}

func openDBWriter(cCtx *cli.Context) (*database.DB, error) {
	sqlite, err := openSQLiteDB(cCtx, sqlu.ParamModeRO)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	githubToken, err := readGithubTokenFile(cCtx)
	if err != nil {
		return nil, fmt.Errorf("read github token file: %w", err)
	}

	db, err := database.NewDB(
		cCtx.Context,
		sqlite,
		postgresFlag.Get(cCtx),
		nextCrawlSuccessFlag.Get(cCtx),
		nextCrawlFailFlag.Get(cCtx),
		nextCrawlNotEthFlag.Get(cCtx),
		githubToken,
	)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres failed: %w", err)
	}

	return db, nil
}

func openDBReader(cCtx *cli.Context) (*database.DB, error) {
	db, err := database.NewAPIDB(
		cCtx.Context,
		nil,
		postgresFlag.Get(cCtx),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres failed: %w", err)
	}

	return db, nil
}
