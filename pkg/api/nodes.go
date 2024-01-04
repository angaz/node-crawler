package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"log/slog"

	"github.com/a-h/templ"
	"github.com/ethereum/node-crawler/public"
	"github.com/jackc/pgx/v5"
)

func (a *API) nodesHandler(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")

	// /nodes/{id}
	if len(pathParts) != 3 {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	nodeID := pathParts[2]

	// /nodes/
	if nodeID == "" {
		a.nodesListHandler(w, r)

		return
	}

	nodes, err := a.db.GetNodeTable(r.Context(), nodeID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		slog.Error("get node page failed", "err", err, "id", nodeID)

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	sb := new(strings.Builder)

	var page templ.Component

	if nodes != nil {
		page = public.NodeTable(*nodes)
	} else {
		page = public.NotFound()
	}

	index := public.Index(public.URLFromReq(r), page, 1, -1)
	_ = index.Render(r.Context(), sb)

	// This is the worst, but templating the style attribute is
	// not allowed for security concerns.
	out := strings.ReplaceAll(
		sb.String(),
		"STYLE_REPLACE",
		"style",
	)

	w.Write([]byte(out))
}
