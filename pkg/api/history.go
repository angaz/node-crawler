package api

import (
	"fmt"
	"net/http"
	"time"

	"log/slog"

	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/public"
)

func (a *API) handleHistoryList(w http.ResponseWriter, r *http.Request) {
	var before, after *time.Time

	query := r.URL.Query()
	networkIDStr := query.Get("network")
	isErrorStr := query.Get("error")
	beforeStr := query.Get("before")
	afterStr := query.Get("after")

	networkID, ok := parseNetworkID(w, networkIDStr)
	if !ok {
		return
	}

	isError, ok := parseErrorParam(w, isErrorStr)
	if !ok {
		return
	}

	if beforeStr != "" {
		beforeT, err := time.ParseInLocation(database.DateTimeLocal, beforeStr, time.UTC)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(
				w,
				"bad before value: %s. Must be YYYY-MM-DDThh:mm:ss format. It will be interpreted in UTC.\n",
				beforeStr,
			)

			return
		}

		before = &beforeT
	}

	if afterStr != "" {
		afterT, err := time.ParseInLocation(database.DateTimeLocal, afterStr, time.UTC)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(
				w,
				"bad after value: %s. Must be YYYY-MM-DDThh:mm:ss format. It will be interpreted in UTC.\n",
				afterStr,
			)

			return
		}

		after = &afterT
	}

	if beforeStr == "" && afterStr == "" {
		afterT := time.Now().UTC()
		after = &afterT
	}

	historyList, err := a.db.GetHistoryList(r.Context(), before, after, networkID, isError)
	if err != nil {
		slog.Error("get history list failed", "err", err)

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

	index := public.Index(
		reqURL,
		public.HistoryList(reqURL, *historyList, ephemeryNetworks),
		networkID,
		1,
	)
	_ = index.Render(r.Context(), w)
}
