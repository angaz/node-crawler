package api

import (
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/database"
)

type API struct {
	db                   *database.DB
	statsUpdateFrequency time.Duration
	enodes               []string
	snapshotDir          string

	stats      database.AllStats
	statsLock  sync.RWMutex
	statsCache map[string]CachedPage
}

func New(
	db *database.DB,
	statsUpdateFrequency time.Duration,
	enodes []string,
	snapshotDir string,
) *API {
	api := &API{
		db:                   db,
		statsUpdateFrequency: statsUpdateFrequency,
		enodes:               enodes,
		snapshotDir:          snapshotDir,

		stats:      database.AllStats{},
		statsLock:  sync.RWMutex{},
		statsCache: map[string]CachedPage{},
	}

	return api
}

type CachedPage struct {
	Page       []byte
	ValidUntil time.Time
}

func (a *API) replaceStats(newStats database.AllStats) {
	a.statsLock.Lock()
	defer a.statsLock.Unlock()

	a.stats = newStats
	a.statsCache = map[string]CachedPage{}
}

func (a *API) getStats() database.AllStats {
	a.statsLock.RLock()
	defer a.statsLock.RUnlock()

	return a.stats
}

func (a *API) getCache(params string) ([]byte, bool) {
	a.statsLock.RLock()
	defer a.statsLock.RUnlock()

	b, ok := a.statsCache[params]
	if !ok {
		return nil, false
	}

	if b.ValidUntil.Before(time.Now()) {
		return nil, false
	}

	return b.Page, true
}

func (a *API) setCache(params string, b []byte, validUntil time.Time) {
	a.statsLock.Lock()
	defer a.statsLock.Unlock()

	a.statsCache[params] = CachedPage{
		Page:       b,
		ValidUntil: validUntil,
	}
}

func (a *API) StartServer(wg *sync.WaitGroup, address string) {
	defer wg.Done()

	router := http.NewServeMux()

	router.HandleFunc("/", a.handleRoot)
	router.HandleFunc("/favicon.ico", handleFavicon)
	router.HandleFunc("/help/", a.handleHelp)
	router.HandleFunc("/history/", a.handleHistoryList)
	router.HandleFunc("/nodes/", a.nodesHandler)
	router.HandleFunc("/snapshots/", a.handleSnapshots)
	router.HandleFunc("/static/", handleStatic)

	log.Info("Starting API", "address", address)
	_ = http.ListenAndServe(address, router)
}
