package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/api"
	"github.com/ethereum/node-crawler/pkg/apidb"
	"github.com/ethereum/node-crawler/pkg/crawlerdb"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/urfave/cli/v2"
)

var (
	apiCommand = &cli.Command{
		Name:   "api",
		Usage:  "API server for the crawler",
		Action: startAPI,
		Flags: []cli.Flag{
			&apiDBFlag,
			&apiListenAddrFlag,
			&autovacuumFlag,
			&busyTimeoutFlag,
			&crawlerDBFlag,
			&dropNodesTimeFlag,
		},
	}
)

func startAPI(ctx *cli.Context) error {
	autovacuum := ctx.String(autovacuumFlag.Name)
	busyTimeout := ctx.Uint64(busyTimeoutFlag.Name)

	crawlerDB, err := openSQLiteDB(
		ctx.String(crawlerDBFlag.Name),
		autovacuum,
		busyTimeout,
	)
	if err != nil {
		return err
	}

	apiDBPath := ctx.String(apiDBFlag.Name)
	shouldInit := false
	if _, err := os.Stat(apiDBPath); os.IsNotExist(err) {
		shouldInit = true
	}
	nodeDB, err := openSQLiteDB(
		apiDBPath,
		autovacuum,
		busyTimeout,
	)
	if err != nil {
		return err
	}
	if shouldInit {
		log.Info("DB did not exist, init")
		if err := apidb.CreateDB(nodeDB); err != nil {
			return err
		}
	}

	sqlite, err := initDB(
		crawlerDBFlag.Get(ctx)+"_v2",
		autovacuumFlag.Get(ctx),
		busyTimeoutFlag.Get(ctx),
	)
	if err != nil {
		return fmt.Errorf("init db failed: %w", err)
	}
	defer sqlite.Close()

	dbv2 := database.NewDB(sqlite, nil)

	var wg sync.WaitGroup
	wg.Add(3)

	// Start reading deamon
	go newNodeDeamon(&wg, crawlerDB, nodeDB)
	go dropDeamon(&wg, nodeDB, ctx.Duration(dropNodesTimeFlag.Name))

	// Start the API deamon
	apiAddress := ctx.String(apiListenAddrFlag.Name)
	apiDeamon := api.New(apiAddress, nodeDB, dbv2)
	go apiDeamon.HandleRequests(&wg)
	wg.Wait()

	return nil
}

func transferNewNodes(crawlerDB, nodeDB *sql.DB) error {
	crawlerDBTx, err := crawlerDB.Begin()
	if err != nil {
		// Sometimes error occur trying to read the crawler database, but
		// they are normally recoverable, and a lot of the time, it's
		// because the database is locked by the crawler.
		return fmt.Errorf("error starting transaction to read nodes: %w", err)
	}
	defer crawlerDBTx.Rollback()

	nodes, err := crawlerdb.ReadAndDeleteUnseenNodes(crawlerDBTx)
	if err != nil {
		// Simiar to nodeDB.Begin() error
		return fmt.Errorf("error reading nodes: %w", err)
	}

	if len(nodes) > 0 {
		err := apidb.InsertCrawledNodes(nodeDB, nodes)
		if err != nil {
			// This shouldn't happen because the database is not shared in this
			// instance, so there shouldn't be lock errors, but anything can
			// happen. We will still try again.
			return fmt.Errorf("error inserting nodes: %w", err)
		}
		log.Info("Nodes inserted", "len", len(nodes))
	}

	crawlerDBTx.Commit()
	return nil
}

// newNodeDeamon reads new nodes from the crawler and puts them in the db
// Might trigger the invalidation of caches for the api in the future
func newNodeDeamon(wg *sync.WaitGroup, crawlerDB, nodeDB *sql.DB) {
	defer wg.Done()

	// This is so that we can make some kind of exponential backoff for the
	// retries.
	retryTimeout := time.Minute

	for {
		err := transferNewNodes(crawlerDB, nodeDB)
		if err != nil {
			log.Error("Failure in transferring new nodes", "err", err)
			time.Sleep(retryTimeout)
			retryTimeout *= 2
			continue
		}

		retryTimeout = time.Minute
		time.Sleep(time.Second)
	}
}

func dropDeamon(wg *sync.WaitGroup, db *sql.DB, dropTimeout time.Duration) {
	defer wg.Done()
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		<-ticker.C
		err := apidb.DropOldNodes(db, dropTimeout)
		if err != nil {
			panic(err)
		}
	}
}
