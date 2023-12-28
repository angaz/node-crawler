package api

import (
	"fmt"
	"net/http"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/public"
)

func (a *API) nodesListHandler(w http.ResponseWriter, r *http.Request) {
	urlQuery := r.URL.Query()

	pageNumStr := urlQuery.Get("page")
	networkIDStr := urlQuery.Get("network")
	syncedStr := urlQuery.Get("synced")
	query := urlQuery.Get("q")
	clientName := urlQuery.Get("client-name")
	clientUserData := urlQuery.Get("client-user-data")

	// This is a full node ID, just redirect to the node's page
	if len(query) == 64 {
		http.Redirect(w, r, "/nodes/"+query, http.StatusTemporaryRedirect)

		return
	}

	pageNumber, ok := parsePageNum(w, pageNumStr)
	if !ok {
		return
	}

	networkID, ok := parseNetworkID(w, networkIDStr)
	if !ok {
		return
	}

	synced, ok := parseSyncedParam(w, syncedStr)
	if !ok {
		return
	}

	nodeType, ok := parseNodeType(w, r)
	if !ok {
		return
	}

	nodeListQuery, err := database.ParseNodeListQuery(query)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `Invalid query: "%s". Not a valid IP address, nor hex string.`, query)

		return
	}

	nodes, err := a.db.GetNodeList(
		r.Context(),
		pageNumber,
		networkID,
		synced,
		*nodeListQuery,
		clientName,
		clientUserData,
		nodeType,
	)
	if err != nil {
		log.Error("get node list failed", "err", err, "pageNumber", pageNumber)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	reqURL := public.URLFromReq(r)

	nodeList := public.NodeList(reqURL, *nodes)
	index := public.Index(reqURL, nodeList, networkID, synced)
	_ = index.Render(r.Context(), w)
}
