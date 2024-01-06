package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var (
	//nolint:exhaustruct  // We don't need to specify everything.
	migrateCommand = &cli.Command{
		Name:   "migrate",
		Usage:  "Run the database migrations",
		Action: migrateAction,
		Flags: []cli.Flag{
			&postgresFlag,
			&geoipdbFlag,
			&crawlerDBFlag,
			&statsDBFlag,
			&githubTokenFileFlag,
		},
	}
)

func migrateAction(cCtx *cli.Context) error {
	geoipDB, err := openGeoIP(cCtx)
	if err != nil {
		return fmt.Errorf("open geoip2 failed: %w", err)
	}

	if geoipDB != nil {
		defer geoipDB.Close()
	}

	db, err := openDBWriter(cCtx, geoipDB)
	if err != nil {
		return fmt.Errorf("open db failed: %w", err)
	}
	defer db.Close()

	return nil
}
