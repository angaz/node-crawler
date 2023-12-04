package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
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
		log.Error("get history list failed", "err", err)
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	reqURL := public.URLFromReq(r)

	index := public.Index(
		reqURL,
		public.HistoryList(reqURL, *historyList),
		networkID,
		1,
	)

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(sb.String(), "STYLE_REPLACE", "style")

	_, _ = w.Write([]byte(out))
}
