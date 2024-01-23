package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/public"
)

type portalStatsParams struct {
	clientName    string
	graphInterval time.Duration
	graphFormat   string
}

func parsePortalStatsParams(w http.ResponseWriter, r *http.Request) *portalStatsParams {
	query := r.URL.Query()
	clientName := query.Get("client-name")

	graphInterval, ok := parseGraphInterval(w, query.Get("interval"))
	if !ok {
		return nil
	}

	graphFormat, ok := parseGraphFormat(w, query.Get("graph-format"))
	if !ok {
		return nil
	}

	return &portalStatsParams{
		clientName:    clientName,
		graphInterval: graphInterval,
		graphFormat:   graphFormat,
	}
}

func (a *API) handlePortalStats(w http.ResponseWriter, r *http.Request) {
	params := parseStatsParams(w, r)
	if params == nil {
		return
	}

	before := time.Now().Add(-params.graphInterval)
	after := before.Add(-3 * 24 * time.Hour)

	switch params.graphInterval {
	case database.GraphInterval3Hour:
		after = before.Add(-4 * 24 * time.Hour)
	case database.GraphInterval24Hour:
		after = before.Add(-28 * 24 * time.Hour)
	}

	stats, err := a.db.GetPortalStats(
		r.Context(),
		after,
		before,
		params.clientName,
		params.graphInterval,
	)
	if err != nil {
		slog.Error("get portal stats failed", "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	reqURL := public.URLFromReq(r)

	graphs := make([]templ.Component, 0, 2)
	instant := make([]templ.Component, 0, 4)

	if params.clientName == "" {
		clientNamesTimeseries := stats.ClientNamesTimeseries()

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
				database.ToInstant(stats.ClientNamesInstant),
				func(key string) templ.SafeURL {
					return reqURL.
						WithParam("client-name", key).
						SafeURL()
				},
			),
		)
	} else {
		clientVersionsTimeseries := stats.ClientNamesTimeseries()

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
				database.ToInstant(stats.ClientNamesInstant),
				func(_ string) templ.SafeURL { return "" },
			),
		)
	}

	instant = append(
		instant,
		public.StatsGroup(
			"Countries",
			database.ToInstant(stats.CountriesInstant),
			func(_ string) templ.SafeURL { return "" },
		),
	)

	clientDialSuccessTimeseries := stats.DialSuccessTimeseries()

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

	statsPage := public.PortalStats(
		reqURL,
		params.clientName,
		graphs,
		instant,
		len(stats.Buckets) == 0,
	)

	index := public.Index(reqURL, statsPage, params.networkID, params.synced)

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(
		sb.String(),
		"STYLE_REPLACE",
		"style",
	)

	_, _ = w.Write([]byte(out))
}
