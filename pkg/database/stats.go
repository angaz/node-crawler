package database

import (
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"golang.org/x/exp/slices"
)

// Meant to be run as a goroutine.
//
// Copies the stats into the stats table every `frequency` duration.
func (db *DB) CopyStatsDaemon(frequency time.Duration) {
	for {
		nextRun := time.Now().Truncate(frequency).Add(frequency)
		time.Sleep(time.Until(nextRun))

		err := db.CopyStats()
		if err != nil {
			log.Error("Copy stats failed", "err", err)
		}
	}
}

func (db *DB) CopyStats() error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("copy_stats", start, err)

	_, err = db.db.Exec(
		`
			INSERT INTO stats.crawled_nodes
		
			SELECT
				unixepoch() timestamp,
				crawled.client_name,
				crawled.client_user_data,
				-- If reth, add build data
				CASE
					WHEN crawled.client_name = 'reth' AND instr(crawled.client_build, '-') != 0
					THEN crawled.client_version || '-' || substr(crawled.client_build, 0, instr(crawled.client_build, '-'))
					ELSE crawled.client_version
				END client_version,
				crawled.client_os,
				crawled.client_arch,
				crawled.network_id,
				crawled.fork_id,
				crawled.next_fork_id,
				crawled.country,
				CASE
					WHEN blocks.timestamp IS NULL
					THEN false
					ELSE abs(crawled.updated_at - blocks.timestamp) < 60
				END synced,
				EXISTS (
					SELECT 1
					FROM crawl_history history
					WHERE
						history.node_id = crawled.node_id
						AND history.direction = 'dial'
						AND history.crawled_at > unixepoch('now', '-7 days')
						AND (
							history.error IS NULL
							OR history.error IN (  -- Disconnect Reasons
								'disconnect requested',
								'network error',
								'breach of protocol',
								'useless peer',
								'too many peers',
								'already connected',
								'incompatible p2p protocol version',
								'invalid node identity',
								'client quitting',
								'unexpected identity',
								'connected to self',
								'read timeout',
								'subprotocol error'
							)
						)
				) dial_success,
				COUNT(*) total
			FROM crawled_nodes crawled
			LEFT JOIN discovered_nodes disc ON (
				crawled.node_id = disc.node_id
			)
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE
				disc.last_found > unixepoch('now', '-48 hours')
			GROUP BY
				crawled.client_name,
				crawled.client_user_data,
				client_version,
				crawled.client_os,
				crawled.client_arch,
				crawled.network_id,
				crawled.fork_id,
				crawled.next_fork_id,
				country,
				synced,
				dial_success
		`,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

type Stats struct {
	Timestamp   time.Time
	Client      Client
	NetworkID   *int64
	ForkID      *uint32
	NextForkID  *uint64
	Country     *string
	Synced      bool
	DialSuccess bool
	Total       int64
}

func (s Stats) CountryStr() string {
	if s.Country == nil {
		return Unknown
	}

	return *s.Country
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

func (s AllCountTotal) Timeseries() Timeseries {
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
				if lastTs.Sub(ts).Abs() > 35*time.Minute {
					lastTs = lastTs.Add(30 * time.Minute)
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
			return s.Client.OS + " / " + s.Client.Arch
		},
		filters...,
	)
}

func (s AllStats) GroupArch(filters ...StatsFilterFn) AllCountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Arch
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
