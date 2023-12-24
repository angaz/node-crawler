package main

import (
	"crypto/ecdsa"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/angaz/sqlu"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/oschwald/geoip2-golang"
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
	dsn := sqlu.ConnParams{
		Filename: crawlerDBFlag.Get(cCtx),
		Mode:     mode,
		Pragma: []sqlu.Pragma{
			sqlu.PragmaBusyTimeout(int64(busyTimeoutFlag.Get(cCtx))),
			sqlu.PragmaJournalSizeLimit(128 * 1024 * 1024), // 128MiB
			sqlu.PragmaAutoVacuumIncremental,
			sqlu.PragmaJournalModeWAL,
			sqlu.PragmaSynchronousNormal,
		},
		Attach: []sqlu.AttachParams{
			{
				Filename: statsDBFlag.Get(cCtx),
				Database: "stats",
				Mode:     mode,
				Pragma: []sqlu.Pragma{
					sqlu.PragmaJournalSizeLimit(128 * 1024 * 1024), // 128MiB
					sqlu.PragmaAutoVacuumIncremental,
					sqlu.PragmaJournalModeWAL,
					sqlu.PragmaSynchronousNormal,
				},
			},
		},
	}

	db, err := sql.Open("sqlite", dsn.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	return db, nil
}

func openDBWriter(cCtx *cli.Context, geoipDB *geoip2.Reader) (*database.DB, error) {
	sqlite, err := openSQLiteDB(cCtx, sqlu.ParamModeRWC)
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
		geoipDB,
		nextCrawlSuccessFlag.Get(cCtx),
		nextCrawlFailFlag.Get(cCtx),
		nextCrawlNotEthFlag.Get(cCtx),
		githubToken,
	)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres failed: %w", err)
	}

	err = db.Migrate()
	if err != nil {
		return nil, fmt.Errorf("database migration failed: %w", err)
	}

	return db, nil
}

func openDBReader(cCtx *cli.Context) (*database.DB, error) {
	sqlite, err := openSQLiteDB(cCtx, sqlu.ParamModeRO)
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	db, err := database.NewAPIDB(
		cCtx.Context,
		sqlite,
		postgresFlag.Get(cCtx),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres failed: %w", err)
	}

	return db, nil
}

func readNodeKeys(cCtx *cli.Context) ([]*ecdsa.PrivateKey, error) {
	nodeKeysFileName := nodeKeysFileFlag.Get(cCtx)

	_, err := os.Stat(nodeKeysFileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			keys, err := common.WriteNodeKeys(16, nodeKeysFileName)
			if err != nil {
				return nil, fmt.Errorf("Writing node keys file failed: %w", err)
			}

			return keys, nil
		}

		return nil, fmt.Errorf("Reading node keys file failed: %w", err)
	}

	keys, err := common.ReadNodeKeys(nodeKeysFileName)
	if err != nil {
		return nil, fmt.Errorf("Read node keys file failed: %w", err)
	}

	return keys, nil
}
