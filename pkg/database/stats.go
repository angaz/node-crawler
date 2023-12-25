package database

import (
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"golang.org/x/exp/slices"
)

// Meant to be run as a goroutine.
//
// Copies the stats into the stats table every `frequency` duration.
func (db *DB) CopyStatsDaemon(frequency time.Duration) {
	for {
		// nextRun := time.Now().Truncate(frequency).Add(frequency)
		// time.Sleep(time.Until(nextRun))

		err := db.CopyStats()
		if err != nil {
			log.Error("Copy stats failed", "err", err)
		}

		nextRun := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(nextRun))
	}
}

func (db *DB) CopyStats() error {
	var err error

	defer metrics.ObserveDBQuery("copy_stats", time.Now(), err)

	rows, err := db.db.Query(
		`
			SELECT
				now() timestamp,
				nodes.client_name_id,
				nodes.client_user_data_id,
				nodes.client_version_id,
				nodes.client_os,
				nodes.client_arch,
				nodes.network_id,
				nodes.fork_id,
				nodes.next_fork_id,
				cities.country_geoname_id,
				CASE
					WHEN blocks.timestamp IS NULL
						THEN false
					WHEN nodes.updated_at > blocks.timestamp
						THEN (nodes.updated_at - blocks.timestamp) < INTERVAL '1 minute'
					ELSE false
				END synced,
				EXISTS (
					SELECT 1
					FROM crawler.history
					WHERE
						history.node_id = nodes.node_id
						AND history.direction = 'dial'
						AND history.crawled_at > (now() - INTERVAL '7 days')
						AND (
							history.error IS NULL
							OR history.error < 'DISCONNECT_REASONS'
						)
				) dial_success,
				COUNT(*) total
			FROM execution.nodes
			LEFT JOIN disc.nodes disc USING (node_id)
			LEFT JOIN execution.blocks ON (
				nodes.head_hash = blocks.block_hash
				AND nodes.network_id = blocks.network_id
			)
			LEFT JOIN geoname.cities USING (city_geoname_id)
			WHERE
				disc.last_found > (now() - INTERVAL '48 hours')
			GROUP BY
				nodes.client_name_id,
				nodes.client_user_data_id,
				nodes.client_version_id,
				nodes.client_os,
				nodes.client_arch,
				nodes.network_id,
				nodes.fork_id,
				nodes.next_fork_id,
				cities.country_geoname_id,
				synced,
				dial_success
		`,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}
	defer rows.Close()

	tx, err := db.pg.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(context.Background())

	stmt, err := tx.Prepare(
		context.Background(),
		"copy_stats",
		`
			WITH data AS (
				SELECT
					$1::TIMESTAMPTZ timestamp,
					$2::TEXT client_name,
					$3::TEXT client_user_data,
					$4::TEXT client_version,
					$5::INT client_os,
					$6::INT client_arch,
					$7::BIGINT network_id,
					$8::BIGINT fork_id,
					$9::BIGINT next_fork_id,
					$10::INT country,
					$11::BOOLEAN synced,
					$12::BOOLEAN dial_success,
					$13::BIGINT total
			), client_names AS (
				INSERT INTO stats.client_names (
					client_name
				)
				SELECT DISTINCT client_name FROM data
				WHERE client_name IS NOT NULL
				ON CONFLICT DO NOTHING
			), client_user_data AS (
				INSERT INTO stats.client_user_data (
					client_user_data
				)
				SELECT DISTINCT client_user_data FROM data
				WHERE client_user_data IS NOT NULL
				ON CONFLICT DO NOTHING
			), client_versions AS (
				INSERT INTO stats.client_versions (
					client_version
				)
				SELECT DISTINCT client_version FROM data
				WHERE client_version IS NOT NULL
				ON CONFLICT DO NOTHING
			)

			INSERT INTO stats.crawled_nodes (
				timestamp,
				client_name_id,
				client_user_data_id,
				client_version_id,
				client_os,
				client_arch,
				network_id,
				fork_id,
				next_fork_id,
				country,
				synced,
				dial_success,
				total
			)
			SELECT
				data.timestamp,
				client_names.client_name_id,
				client_user_data.client_user_data_id,
				client_versions.client_version_id,
				data.client_os,
				data.client_arch,
				data.network_id,
				data.fork_id,
				data.next_fork_id,
				data.country,
				data.synced,
				data.dial_success,
				data.total
			FROM data
			LEFT JOIN stats.client_names
				ON (data.client_name = client_names.client_name)
			LEFT JOIN stats.client_user_data
				ON (data.client_user_data = client_user_data.client_user_data)
			LEFT JOIN stats.client_versions
				ON (data.client_version = client_versions.client_version)
		`,
	)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}

	for rows.Next() {
		var timestamp int64
		var clientName string
		var clientUserData *string
		var clientVersion *string
		var clientOS string
		var clientArch string
		var networkID int64
		var forkID int64
		var nextForkID uint64
		var country string
		var synced bool
		var dialSuccess bool
		var total int64

		err := rows.Scan(
			&timestamp,
			&clientName,
			&clientUserData,
			&clientVersion,
			&clientOS,
			&clientArch,
			&networkID,
			&forkID,
			&nextForkID,
			&country,
			&synced,
			&dialSuccess,
			&total,
		)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		_, err = tx.Exec(
			context.Background(),
			stmt.Name,

			time.Unix(timestamp, 0),
			clientName,
			clientUserData,
			clientVersion,
			// TODO
			// slices.Index(osStrings, clientOS),
			// slices.Index(archStrings, clientArch),
			0,
			0,
			networkID,
			forkID,
			nextForkID,
			// TODO
			// ParseCountryName(country),
			0,
			synced,
			dialSuccess,
			total,
		)
		if err != nil {
			return fmt.Errorf("exec: %w", err)
		}
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

type Stats struct {
	Timestamp   time.Time
	Client      common.Client
	NetworkID   int64
	ForkID      uint32
	NextForkID  *uint64
	Country     string
	Synced      bool
	DialSuccess bool
	Total       int64
}

type statsJSON struct {
	Timestamp      time.Time `json:"timestamp"`
	ClientName     string    `json:"client_name"`
	ClientUserData *string   `json:"client_user_data"`
	ClientVersion  string    `json:"client_version"`
	ClientOS       string    `json:"client_os"`
	ClientArch     string    `json:"client_arch"`
	NetworkID      int64     `json:"network_id"`
	ForkID         string    `json:"fork_id"`
	NextForkID     *string   `json:"next_fork_id"`
	Country        string    `json:"country"`
	Synced         bool      `json:"synced"`
	DialSuccess    bool      `json:"dial_success"`
	Total          int64     `json:"total"`
}

func (s Stats) MarshalJSON() ([]byte, error) {
	var userData *string
	if s.Client.UserData != common.Unknown {
		userData = &s.Client.UserData
	}

	sj := statsJSON{
		Timestamp:      s.Timestamp.UTC(),
		ClientName:     s.Client.Name,
		ClientUserData: userData,
		ClientVersion:  s.Client.Version,
		ClientOS:       s.Client.OS.String(),
		ClientArch:     s.Client.Arch.String(),
		NetworkID:      s.NetworkID,
		ForkID:         s.ForkIDStr(),
		NextForkID:     s.NextForkIDStr(),
		Country:        s.Country,
		Synced:         s.Synced,
		DialSuccess:    s.DialSuccess,
		Total:          s.Total,
	}

	return json.Marshal(sj)
}

func (s Stats) CountryStr() string {
	return s.Country
}

func (s Stats) ForkIDStr() string {
	return common.Uint32ToForkID(s.ForkID).Hex()
}

func (s Stats) NextForkIDStr() *string {
	if s.NextForkID == nil {
		return nil
	}

	var id [8]byte
	binary.BigEndian.PutUint64(id[:], *s.NextForkID)

	hexStr := hex.EncodeToString(id[:])

	return &hexStr
}

type AllStats []Stats

type KeyFn func(Stats) string
type StatsFilterFn func(int, Stats) bool

func (s AllStats) LastStats() AllStats {
	if len(s) == 0 {
		return AllStats{}
	}

	lastTs := s[len(s)-1].Timestamp

	for i := len(s) - 1; i > 0; i-- {
		if s[i].Timestamp != lastTs {
			return s[i:]
		}
	}

	return s
}

func (s AllStats) Filter(filters ...StatsFilterFn) AllStats {
	// Should be one malloc on most requests, and one more at most.
	out := make(AllStats, 0, 16384)

	for i, stat := range s {
		skip := false
		for _, filter := range filters {
			if !filter(i, stat) {
				skip = true
				break
			}
		}

		if !skip {
			out = append(out, stat)
		}
	}

	return out
}

func (s AllStats) GroupBy(keyFn KeyFn, filters ...StatsFilterFn) AllCountTotal {
	allCount := map[time.Time]map[string]int64{}

	for _, stat := range s.Filter(filters...) {
		ts := stat.Timestamp
		key := keyFn(stat)

		_, ok := allCount[ts]
		if !ok {
			allCount[ts] = map[string]int64{}
		}

		v, ok := allCount[ts][key]
		if !ok {
			v = 0
		}

		v += stat.Total
		allCount[ts][key] = v
	}

	allOut := make([]CountTotal, 0, len(allCount))

	for ts, count := range allCount {
		out := make([]Count, 0, len(count))
		var total int64 = 0

		for key, value := range count {
			out = append(out, Count{
				Key:   key,
				Count: value,
			})

			total += value
		}

		slices.SortFunc(out, func(a, b Count) int {
			if a.Count == b.Count {
				return strings.Compare(b.Key, a.Key)
			}

			return int(b.Count - a.Count)
		})

		allOut = append(allOut, CountTotal{
			Timestamp: ts,
			Values:    out,
			Total:     total,
		})
	}

	slices.SortFunc(allOut, func(a, b CountTotal) int {
		return a.Timestamp.Compare(b.Timestamp)
	})

	return allOut
}

type Count struct {
	Key   string
	Count int64
}

type CountTotal struct {
	Timestamp time.Time
	Values    []Count
	Total     int64
}

type AllCountTotal []CountTotal

func (s AllCountTotal) Last() CountTotal {
	if len(s) == 0 {
		return CountTotal{
			Timestamp: time.Time{},
			Values:    []Count{},
			Total:     0,
		}
	}

	return s[len(s)-1]
}

type ChartSeriesEmphasis struct {
	Focus string `json:"focus"`
}

type ChartXAxis struct {
	Type       string   `json:"type"`
	BoundryGap bool     `json:"boundryGap"`
	Data       []string `json:"data"`
}

type ChartSeries struct {
	Name      string              `json:"name"`
	Type      string              `json:"type"`
	Colour    string              `json:"color,omitempty"`
	Stack     string              `json:"stack"`
	AreaStyle struct{}            `json:"areaStyle"`
	Emphasis  ChartSeriesEmphasis `json:"emphasis"`
	Data      []*float64          `json:"data"`
}

type Timeseries struct {
	Legend   []string      `json:"legend"`
	Series   []ChartSeries `json:"series"`
	XAxis    []ChartXAxis  `json:"xAxis"`
	YAxisMax *float64      `json:"yAxisMax"`
}

func newFloat(i int64) *float64 {
	f := float64(i)
	return &f
}

func (ts Timeseries) Percentage() Timeseries {
	if len(ts.Series) < 2 {
		return ts
	}

	for i := 0; i < len(ts.Series[0].Data); i++ {
		var total float64 = 0.0

		for _, series := range ts.Series {
			value := series.Data[i]

			if value != nil {
				total += *value
			}
		}

		for _, series := range ts.Series {
			if series.Data[i] != nil {
				*series.Data[i] = *series.Data[i] / total * 100
			}
		}
	}

	ts.YAxisMax = newFloat(100)

	return ts
}

func (s AllCountTotal) Timeseries(interval time.Duration) Timeseries {
	timestampMap := map[time.Time]struct{}{}

	for _, c := range s {
		timestampMap[c.Timestamp] = struct{}{}
	}

	timestamps := make([]time.Time, 0, len(timestampMap))

	for ts := range timestampMap {
		timestamps = append(timestamps, ts)
	}

	slices.SortFunc(timestamps, func(a, b time.Time) int {
		return a.Compare(b)
	})

	outTs := make([]time.Time, 0, len(timestamps))

	if len(timestamps) > 0 {
		lastTs := timestamps[0]
		outTs = append(outTs, lastTs)

		for _, ts := range timestamps[1:] {
			for {
				if lastTs.Sub(ts).Abs() > interval {
					lastTs = lastTs.Add(interval)
					outTs = append(outTs, lastTs)
				} else {
					outTs = append(outTs, ts)
					lastTs = ts

					break
				}
			}
		}
	}

	timeseries := map[string][]*float64{}

	for i, ts := range outTs {
		for _, e := range s {
			if e.Timestamp.Equal(ts) {
				for _, ee := range e.Values {
					data, ok := timeseries[ee.Key]
					if ok {
						data[i] = newFloat(ee.Count)
					} else {
						data := make([]*float64, len(outTs))
						data[i] = newFloat(ee.Count)
						timeseries[ee.Key] = data
					}
				}
			}
		}
	}

	chartSeries := make([]ChartSeries, 0, len(timeseries))
	legend := make([]string, 0, len(timeseries))

	for key, series := range timeseries {
		legend = append(legend, key)

		chartSeries = append(chartSeries, ChartSeries{
			Name:      key,
			Type:      "line",
			Colour:    "",
			Stack:     "Total",
			AreaStyle: struct{}{},
			Emphasis: ChartSeriesEmphasis{
				Focus: "series",
			},
			Data: series,
		})
	}

	tsStr := make([]string, len(outTs))
	for i, ts := range outTs {
		tsStr[i] = ts.UTC().Format("2006-01-02 15:04")
	}

	slices.SortStableFunc(legend, strings.Compare)

	slices.SortFunc(chartSeries, func(a, b ChartSeries) int {
		aData := a.Data[len(a.Data)-1]
		bData := b.Data[len(b.Data)-1]

		if aData == bData {
			return 0
		}

		if bData == nil && aData != nil {
			return 1
		}

		if aData == nil && bData != nil {
			return -1
		}

		if *aData > *bData {
			return 1
		}

		return -1
	})

	return Timeseries{
		Legend: legend,
		Series: chartSeries,
		XAxis: []ChartXAxis{
			{
				Type:       "category",
				BoundryGap: false,
				Data:       tsStr,
			},
		},
		YAxisMax: nil,
	}
}

func (t Timeseries) Colours(colours ...string) Timeseries {
	for i := range t.Series {
		t.Series[i].Colour = colours[i%len(colours)]
	}

	return t
}

func (t CountTotal) Limit(limit int) CountTotal {
	return CountTotal{
		Timestamp: time.Time{},
		Values:    t.Values[:min(limit, len(t.Values))],
		Total:     t.Total,
	}
}

type CountTotalOrderFn func(a, b Count) int

func (t CountTotal) OrderBy(orderByFn CountTotalOrderFn) CountTotal {
	slices.SortStableFunc(t.Values, orderByFn)

	return CountTotal{
		Timestamp: time.Time{},
		Values:    t.Values,
		Total:     t.Total,
	}
}

func (s AllStats) GroupClientName(filters ...StatsFilterFn) AllCountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Name
		},
		filters...,
	)
}

func (s AllStats) GroupDialSuccess(filters ...StatsFilterFn) AllCountTotal {
	return s.GroupBy(
		func(s Stats) string {
			if s.DialSuccess {
				return "Success"
			}
			return "Fail"
		},
		filters...,
	)
}

func (s AllStats) GroupCountries(filters ...StatsFilterFn) AllCountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.CountryStr()
		},
		filters...,
	)
}

func (s AllStats) GroupClientVersion(filters ...StatsFilterFn) AllCountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Version
		},
		filters...,
	)
}

func (s AllStats) GroupClientBuild(filters ...StatsFilterFn) AllCountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Build
		},
		filters...,
	)
}

func (s AllStats) GroupOS(filters ...StatsFilterFn) AllCountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.OS.String() + " / " + s.Client.Arch.String()
		},
		filters...,
	)
}

func (s AllStats) GroupArch(filters ...StatsFilterFn) AllCountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Arch.String()
		},
		filters...,
	)
}

func (s AllStats) GroupLanguage(filters ...StatsFilterFn) AllCountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Language
		},
		filters...,
	)
}
