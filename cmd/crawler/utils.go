package main

import (
	"crypto/ecdsa"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/angaz/sqlu"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oschwald/geoip2-golang"
	"github.com/urfave/cli/v2"
)

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

	pg, err := pgxpool.New(cCtx.Context, postgresFlag.Get(cCtx))
	if err != nil {
		return nil, fmt.Errorf("connect to postgres failed: %w", err)
	}

	db := database.NewDB(
		sqlite,
		pg,
		geoipDB,
		nextCrawlSuccessFlag.Get(cCtx),
		nextCrawlFailFlag.Get(cCtx),
		nextCrawlNotEthFlag.Get(cCtx),
	)

	err = db.MigrateCrawler()
	if err != nil {
		return nil, fmt.Errorf("database migration failed: %w", err)
	}

	err = db.MigrateStats()
	if err != nil {
		return nil, fmt.Errorf("stats database migration failed: %w", err)
	}

	err = db.MigrateStatsPG()
	if err != nil {
		return nil, fmt.Errorf("stats pg migration failed: %w", err)
	}

	return db, nil
}

func openDBReader(cCtx *cli.Context) (*database.DB, error) {
	sqlite, err := openSQLiteDB(cCtx, sqlu.ParamModeRO)
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	pg, err := pgxpool.New(cCtx.Context, postgresFlag.Get(cCtx))
	if err != nil {
		return nil, fmt.Errorf("connect to postgres failed: %w", err)
	}

	db := database.NewAPIDB(sqlite, pg)

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
