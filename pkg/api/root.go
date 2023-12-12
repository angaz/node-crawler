package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/public"
)

type statsParams struct {
	clientName string
	networkID  int64
	nextFork   int
	synced     int
}

func (p statsParams) cacheKey() string {
	return fmt.Sprintf(
		"%s,%d,%d,%d",
		p.clientName,
		p.networkID,
		p.synced,
		p.nextFork,
	)
}

func parseStatsParams(w http.ResponseWriter, r *http.Request) *statsParams {
	query := r.URL.Query()
	networkIDStr := query.Get("network")
	syncedStr := query.Get("synced")
	nextForkStr := query.Get("next-fork")
	clientName := query.Get("client-name")

	networkID, found := parseNetworkID(w, networkIDStr)
	if !found {
		return nil
	}

	synced, found := parseSyncedParam(w, syncedStr)
	if !found {
		return nil
	}

	nextFork, found := parseNextForkParam(w, nextForkStr)
	if !found {
		return nil
	}

	return &statsParams{
		clientName: clientName,
		networkID:  networkID,
		nextFork:   nextFork,
		synced:     synced,
	}
}

func (a *API) getFilterStats(
	ctx context.Context,
	params *statsParams,
	before time.Time,
	after time.Time,
) (database.AllStats, error) {
	fork, forkFound := database.Forks[params.networkID]

	allStats, err := a.db.GetStats(
		ctx,
		after,
		before,
		params.networkID,
		params.synced,
		params.clientName,
	)
	if err != nil {
		log.Error("GetStats failed", "err", err)

		return nil, fmt.Errorf("internal server error")
	}

	allStats = allStats.Filter(
		func(_ int, s database.Stats) bool {
			return params.synced == -1 ||
				(params.synced == 1 && s.Synced) ||
				(params.synced == 0 && !s.Synced)
		},
		func(_ int, s database.Stats) bool {
			if params.networkID == -1 {
				return true
			}

			if s.NetworkID != params.networkID {
				return false
			}

			// If fork is not known, keep the stats.
			if !forkFound {
				return true
			}

			// If the fork is known, the fork ID should be in the set.
			_, found := fork.Hash[s.ForkID]
			return found
		},
		func(_ int, s database.Stats) bool {
			if params.nextFork == -1 {
				return true
			}

			if s.NextForkID == nil {
				return false
			}

			// Unknown chain, keep the stats.
			if !forkFound {
				return true
			}

			// Fork time unknown, keep the stats.
			if fork.NextFork == nil {
				return true
			}

			isReady := *s.NextForkID == *fork.NextFork

			return isReady == (params.nextFork == 1)
		},
		func(_ int, s database.Stats) bool {
			if params.clientName == "" {
				return true
			}

			return s.Client.Name == params.clientName
		},
	)

	return allStats, nil
}

type statsResp struct {
	Stats database.AllStats `json:"stats"`
}

func (a *API) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	params := parseStatsParams(w, r)
	if params == nil {
		return
	}

	var before, after time.Time
	var ok bool

	query := r.URL.Query()

	if !query.Has("before") && !query.Has("after") {
		before = time.Now()
		after = before.Add(-30 * time.Minute)
	} else {
		before, ok = parseTimeParam(w, "before", query.Get("before"))
		if !ok {
			return
		}

		after, ok = parseTimeParam(w, "after", query.Get("after"))
		if !ok {
			return
		}
	}

	if after.After(before) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "before shoud be after the after param")

		return
	}

	if after.Sub(before) > 12*time.Hour {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "time range more than 12 hours. Please limit your query to this range.")

		return
	}

	allStats, err := a.getFilterStats(r.Context(), params, before, after)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	w.Header().Set("Content-Type", "application/json")

	encoder := json.NewEncoder(w)
	encoder.Encode(statsResp{
		Stats: allStats,
	})
}

func (a *API) handleRoot(w http.ResponseWriter, r *http.Request) {
	params := parseStatsParams(w, r)
	if params == nil {
		return
	}

	b, found := a.getCache(params.cacheKey())
	if found {
		_, _ = w.Write(b)

		return
	}

	days := 3

	// All network ids has so much data, so we're prioritizing speed over days
	// of data.
	if params.networkID == -1 || params.synced == -1 {
		days = 1
	}

	before := time.Now().Truncate(30 * time.Minute).Add(30 * time.Minute)
	after := before.AddDate(0, 0, -days)

	allStats, err := a.getFilterStats(r.Context(), params, before, after)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	reqURL := public.URLFromReq(r)

	graphs := make([]templ.Component, 0, 2)
	last := make([]templ.Component, 0, 4)

	if params.clientName == "" {
		clientNames := allStats.GroupClientName()

		graphs = append(
			graphs,
			public.StatsGraph(
				fmt.Sprintf("Client Names (%dd)", days),
				"client_names",
				clientNames.Timeseries().Percentage(),
			),
		)

		last = append(
			last,
			public.StatsGroup(
				"Client Names",
				clientNames.Last(),
				func(key string) templ.SafeURL {
					return reqURL.
						KeepParams("network", "synced", "next-fork").
						WithParam("client-name", key).
						SafeURL()
				},
			),
		)
	} else {
		clientVersions := allStats.GroupClientVersion()

		graphs = append(
			graphs,
			public.StatsGraph(
				fmt.Sprintf("Client Versions (%dd)", days),
				"client_versions",
				clientVersions.Timeseries().Percentage(),
			),
		)

		last = append(
			last,
			public.StatsGroup(
				"Client Versions",
				clientVersions.Last(),
				func(_ string) templ.SafeURL { return "" },
			),
		)
	}

	countries := allStats.GroupCountries()
	OSs := allStats.GroupOS()

	last = append(
		last,
		public.StatsGroup(
			"Countries",
			countries.Last(),
			func(_ string) templ.SafeURL { return "" },
		),
		public.StatsGroup(
			"OS / Archetectures",
			OSs.Last(),
			func(_ string) templ.SafeURL { return "" },
		),
	)

	dialSuccess := allStats.GroupDialSuccess()

	graphs = append(
		graphs,
		public.StatsGraph(
			fmt.Sprintf("Dial Success (%dd)", days),
			"dial_success",
			dialSuccess.Timeseries().Percentage().Colours("#05c091", "#ff6e76"),
		),
	)

	statsPage := public.Stats(
		reqURL,
		params.networkID,
		params.synced,
		params.nextFork,
		params.clientName,
		graphs,
		last,
		len(allStats) == 0,
	)

	index := public.Index(reqURL, statsPage, params.networkID, params.synced)

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(sb.String(), "STYLE_REPLACE", "style")

	_, _ = w.Write([]byte(out))

	// Cache the result until 1 minute after the end timestamp.
	// The new stats should have been generated by then.
	a.setCache(params.cacheKey(), []byte(out), before.Add(5*time.Minute))
}
