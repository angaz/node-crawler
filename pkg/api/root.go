package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"log/slog"

	"github.com/a-h/templ"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/public"
)

type statsParams struct {
	clientName       string
	networkID        int64
	nextFork         int
	supportsForkName string
	synced           int
	graphInterval    time.Duration
	graphFormat      string
}

func (p statsParams) cacheKey() string {
	return fmt.Sprintf(
		"%s,%d,%d,%d,%s,%d,%s",
		p.clientName,
		p.networkID,
		p.synced,
		p.nextFork,
		p.supportsForkName,
		int(p.graphInterval.Seconds()),
		p.graphFormat,
	)
}

func parseStatsParams(w http.ResponseWriter, r *http.Request) *statsParams {
	query := r.URL.Query()
	clientName := query.Get("client-name")
	supportsForkName := query.Get("supports-fork-name")

	networkID, ok := parseNetworkID(w, query.Get("network"))
	if !ok {
		return nil
	}

	synced, ok := parseSyncedParam(w, query.Get("synced"))
	if !ok {
		return nil
	}

	nextFork, ok := parseNextForkParam(w, query.Get("next-fork"))
	if !ok {
		return nil
	}

	graphInterval, ok := parseGraphInterval(w, query.Get("interval"))
	if !ok {
		return nil
	}

	graphFormat, ok := parseGraphFormat(w, query.Get("graph-format"))
	if !ok {
		return nil
	}

	if supportsForkName != "" && nextFork != -1 {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, "next-fork and supports-fork-name are mutually exclusive. Only one can be set.")

		return nil
	}

	return &statsParams{
		clientName:       clientName,
		networkID:        networkID,
		nextFork:         nextFork,
		supportsForkName: supportsForkName,
		synced:           synced,
		graphInterval:    graphInterval,
		graphFormat:      graphFormat,
	}
}

func (a *API) getFilterStats(
	ctx context.Context,
	after time.Time,
	before time.Time,
	params *statsParams,
) (*database.StatsResult, error) {
	allStats, err := a.db.GetStats(
		ctx,
		after,
		before,
		params.networkID,
		params.synced,
		params.nextFork,
		params.supportsForkName,
		params.clientName,
		params.graphInterval,
	)
	if err != nil {
		slog.Error("GetStats failed", "err", err)

		return nil, fmt.Errorf("internal server error")
	}

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

	stats, err := a.db.GetStatsAPI(
		r.Context(),
		after,
		before,
		params.networkID,
		params.synced,
		params.nextFork,
		params.supportsForkName,
		params.clientName,
	)
	if err != nil {
		slog.Error("get stats api failed", "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	w.Header().Set("Content-Type", "application/json")

	encoder := json.NewEncoder(w)

	err = encoder.Encode(stats)
	if err != nil {
		slog.Error("stats api encode json failed", "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}
}

func (a *API) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "" && r.URL.Path != "/" {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, "1) What\n2) 404 Not Found\n")

		return
	}

	params := parseStatsParams(w, r)
	if params == nil {
		return
	}

	// if r.URL.Query().Get("nocache") == "" {
	// 	b, found := a.getCache(params.cacheKey())
	// 	if found {
	// 		_, _ = w.Write(b)

	// 		return
	// 	}
	// }

	before := time.Now().Add(-params.graphInterval)
	after := before.Add(-3 * 24 * time.Hour)

	switch params.graphInterval {
	case database.GraphInterval3Hour:
		after = before.Add(-4 * 24 * time.Hour)
	case database.GraphInterval24Hour:
		after = before.Add(-28 * 24 * time.Hour)
	}

	allStats, err := a.getFilterStats(r.Context(), after, before, params)
	if err != nil {
		slog.Error("get filter stats failed", "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	ephemeryNetworks, err := a.db.EphemeryNetworks(r.Context())
	if err != nil {
		slog.Error("get ephemery networks failed", "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	reqURL := public.URLFromReq(r)

	graphs := make([]templ.Component, 0, 2)
	instant := make([]templ.Component, 0, 4)

	if params.clientName == "" {
		clientNamesTimeseries := allStats.ClientNamesTimeseries()

		if params.graphFormat == database.GraphFormatPercent {
			clientNamesTimeseries = clientNamesTimeseries.Percentage()
		}

		graphs = append(
			graphs,
			public.StatsGraph(
				"Client Names",
				"client_names",
				clientNamesTimeseries,
				params.graphInterval,
				params.graphFormat,
				func(key string, value string) templ.SafeURL {
					return reqURL.
						WithParam(key, value).
						SafeURL()
				},
			),
		)

		instant = append(
			instant,
			public.StatsGroup(
				"Client Names",
				database.ToInstant(allStats.ClientNamesInstant),
				func(key string) templ.SafeURL {
					return reqURL.
						WithParam("client-name", key).
						SafeURL()
				},
			),
		)
	} else {
		clientVersionsTimeseries := allStats.ClientNamesTimeseries()

		if params.graphFormat == database.GraphFormatPercent {
			clientVersionsTimeseries = clientVersionsTimeseries.Percentage()
		}

		graphs = append(
			graphs,
			public.StatsGraph(
				"Client Versions",
				"client_versions",
				clientVersionsTimeseries,
				params.graphInterval,
				params.graphFormat,
				func(key string, value string) templ.SafeURL {
					return reqURL.
						WithParam(key, value).
						SafeURL()
				},
			),
		)

		instant = append(
			instant,
			public.StatsGroup(
				"Client Versions",
				database.ToInstant(allStats.ClientNamesInstant),
				func(_ string) templ.SafeURL { return "" },
			),
		)
	}

	instant = append(
		instant,
		public.StatsGroup(
			"Countries",
			database.ToInstant(allStats.CountriesInstant),
			func(_ string) templ.SafeURL { return "" },
		),
		public.StatsGroup(
			"OS / Archetectures",
			database.ToInstant(allStats.OSArchInstant),
			func(_ string) templ.SafeURL { return "" },
		),
	)

	clientDialSuccessTimeseries := allStats.DialSuccessTimeseries()

	if params.graphFormat == database.GraphFormatPercent {
		clientDialSuccessTimeseries = clientDialSuccessTimeseries.Percentage()
	}

	graphs = append(
		graphs,
		public.StatsGraph(
			"Dial Success",
			"dial_success",
			clientDialSuccessTimeseries.Colours(map[string]string{
				"Fail":    "#ff6e76",
				"Success": "#05c091",
			}),
			params.graphInterval,
			params.graphFormat,
			func(key string, value string) templ.SafeURL {
				return reqURL.
					WithParam(key, value).
					SafeURL()
			},
		),
	)

	statsPage := public.Stats(
		reqURL,
		params.networkID,
		params.synced,
		params.nextFork,
		params.supportsForkName,
		params.clientName,
		graphs,
		instant,
		len(allStats.Buckets) == 0,
		ephemeryNetworks,
	)

	index := public.Index(reqURL, statsPage, params.networkID, params.synced)

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := sb.String()
	_, _ = w.Write([]byte(out))

	// Cache the result until 5 minutes after the end timestamp.
	// The new stats should have been generated by then.
	// a.setCache(
	// 	params.cacheKey(),
	// 	[]byte(out),
	// 	time.Now().Truncate(30*time.Minute).Add(35*time.Minute),
	// )
}
