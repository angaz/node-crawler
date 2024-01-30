package migrations

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oschwald/geoip2-golang"
	"github.com/oschwald/maxminddb-golang"
)

func upsertGeoname(ctx context.Context, tx pgx.Tx, city geoip2.City) error {
	_, err := tx.Exec(
		ctx,
		`
			WITH country AS (
				INSERT INTO geoname.countries (
					country_geoname_id,
					country_name
				)
				VALUES (
					@country_geoname_id,
					@country_name
				)
				ON CONFLICT (country_geoname_id) DO NOTHING
			)

			INSERT INTO geoname.cities (
				city_geoname_id,
				city_name,
				country_geoname_id,
				latitude,
				longitude
			)
			VALUES (
				@city_geoname_id,
				@city_name,
				@country_geoname_id,
				@latitude,
				@longitude
			)
			ON CONFLICT (city_geoname_id) DO NOTHING
		`,
		pgx.NamedArgs{
			"country_geoname_id": city.Country.GeoNameID,
			"country_name":       city.Country.Names["en"],
			"city_geoname_id":    city.City.GeoNameID,
			"city_name":          city.City.Names["en"],
			"latitude":           city.Location.Latitude,
			"longitude":          city.Location.Longitude,
		},
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

type networkCity struct {
	network       net.IPNet
	cityGeonameID uint
}

func replaceGeoIPData(ctx context.Context, tx pgx.Tx, networks []networkCity) error {
	_, err := tx.Exec(
		ctx,
		`
			CREATE TABLE geoip.networks_new
				(LIKE geoip.networks INCLUDING ALL)
		`,
	)
	if err != nil {
		return fmt.Errorf("create new table: %w", err)
	}

	copy := pgx.CopyFromSlice(len(networks), func(i int) ([]any, error) {
		n := networks[i]

		return []any{
			n.network,
			n.cityGeonameID,
		}, nil
	})

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"geoip", "networks_new"},
		[]string{
			"network",
			"city_geoname_id",
		},
		copy,
	)
	if err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
			ALTER TABLE geoip.networks RENAME TO networks_old;
			ALTER TABLE geoip.networks_new RENAME TO networks;
			DROP TABLE geoip.networks_old CASCADE;
		`,
	)
	if err != nil {
		return fmt.Errorf("swap tables: %w", err)
	}

	return nil
}

func cityDBIsNewer(ctx context.Context, tx pgx.Tx, ts time.Time) (bool, error) {
	row := tx.QueryRow(
		ctx,
		`
			SELECT build_timestamp
			FROM geoip.current_build
		`,
	)

	var buildTime time.Time

	err := row.Scan(&buildTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil
		}

		return false, fmt.Errorf("query: %w", err)
	}

	return ts.After(buildTime), nil
}

func setGeoIPBuildTs(ctx context.Context, tx pgx.Tx, ts time.Time) error {
	_, err := tx.Exec(
		ctx,
		`
			WITH del AS (
				DELETE FROM geoip.current_build
			)

			INSERT INTO geoip.current_build (
				build_timestamp
			) VALUES (
				$1
			)
		`,
		ts,
	)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}

func UpdateGeoIPData(ctx context.Context, tx pgx.Tx, geoipdb string) error {
	start := time.Now()

	if geoipdb == "" {
		return nil
	}

	cityDB, err := maxminddb.Open(geoipdb)
	if err != nil {
		return fmt.Errorf("open geoipdb: %w", err)
	}
	defer cityDB.Close()

	if cityDB == nil {
		return nil
	}

	buildTimestamp := time.Unix(int64(cityDB.Metadata.BuildEpoch), 0)

	isNewer, err := cityDBIsNewer(ctx, tx, buildTimestamp)
	if err != nil {
		return fmt.Errorf("is newer: %w", err)
	}

	if !isNewer {
		return nil
	}

	networks := make([]networkCity, 0, 1024)
	cities := make(map[uint]geoip2.City, 1024)

	networkIter := cityDB.Networks(maxminddb.SkipAliasedNetworks)

	for networkIter.Next() {
		var city geoip2.City

		subnet, err := networkIter.Network(&city)
		if err != nil {
			return fmt.Errorf("get network: %w", err)
		}

		if city.City.GeoNameID != 0 {
			cities[city.City.GeoNameID] = city
			networks = append(
				networks,
				networkCity{
					network:       *subnet,
					cityGeonameID: city.City.GeoNameID,
				},
			)
		}
	}

	for _, city := range cities {
		err := upsertGeoname(ctx, tx, city)
		if err != nil {
			return fmt.Errorf("upsert geoname: %w", err)
		}
	}

	err = replaceGeoIPData(ctx, tx, networks)
	if err != nil {
		return fmt.Errorf("replace geoip data: %w", err)
	}

	err = setGeoIPBuildTs(ctx, tx, buildTimestamp)
	if err != nil {
		return fmt.Errorf("set geoip build timestamp: %w", err)
	}

	slog.Info("geoip updated", "duration", time.Since(start))

	err = ExecutionNodeView(ctx, tx)
	if err != nil {
		return fmt.Errorf("replace execution node view: %w", err)
	}

	err = PortalNodesView(ctx, tx)
	if err != nil {
		return fmt.Errorf("replace portal nodes view: %w", err)
	}

	return nil
}
