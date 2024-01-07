package api

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"slices"
	"strings"

	"log/slog"

	"github.com/ethereum/node-crawler/public"
)

func allFiles(dirName string) ([]fs.FileInfo, error) {
	dir, err := os.ReadDir(dirName)
	if err != nil {
		return nil, fmt.Errorf("reading dir: %s failed: %w", dirName, err)
	}

	files := []fs.FileInfo{}
	for _, file := range dir {
		if file.IsDir() {
			continue
		}

		// Not a complete snapshot
		if !strings.HasSuffix(file.Name(), ".pgdump") {
			continue
		}

		info, err := file.Info()
		if err != nil {
			return nil, fmt.Errorf("file info for: %s failed: %w", file.Name(), err)
		}

		files = append(files, info)
	}

	slices.SortStableFunc(files, func(a, b fs.FileInfo) int {
		return strings.Compare(b.Name(), a.Name())
	})

	return files, nil
}

func (a *API) handleSnapshotsList(w http.ResponseWriter, r *http.Request) {
	files, err := allFiles(a.snapshotDir)
	if err != nil {
		slog.Error("listing snapshots failed", "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Internal Server Error\n")

		return
	}

	snapshotList := public.SnapshotsList(files)

	index := public.Index(public.URLFromReq(r), snapshotList, 1, 1)
	_ = index.Render(r.Context(), w)
}

func (a *API) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	urlPath := r.URL.Path

	split := strings.Split(strings.Trim(urlPath, "/"), "/")

	if len(split) == 1 {
		a.handleSnapshotsList(w, r)

		return
	}

	if len(split) == 2 {
		fileServer := http.FileServer(http.Dir(a.snapshotDir))

		rCopy := r.Clone(r.Context())
		rCopy.URL.Path = split[1]
		fileServer.ServeHTTP(w, rCopy)

		return
	}

	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprint(w, "Bad Request")
}
