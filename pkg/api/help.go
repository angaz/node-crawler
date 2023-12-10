package api

import (
	"math/rand"
	"net/http"

	"github.com/ethereum/node-crawler/public"
)

func randomEnode(enodes []string) string {
	return enodes[rand.Intn(len(enodes))]
}

func (a *API) handleHelp(w http.ResponseWriter, r *http.Request) {
	helpPage := public.HelpPage(a.enodes, randomEnode(a.enodes))

	index := public.Index(public.URLFromReq(r), helpPage, 1, -1)
	_ = index.Render(r.Context(), w)
}
