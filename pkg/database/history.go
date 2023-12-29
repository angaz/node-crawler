package database

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/jackc/pgx/v5"
)

type HistoryListRow struct {
	NodeID           string
	ClientIdentifier *string
	NetworkID        *int64
	CrawledAt        time.Time
	Direction        string
	Error            *string
}

func (r HistoryListRow) CrawledAtStr() string {
	return r.CrawledAt.UTC().Format(time.RFC3339)
}

func (r HistoryListRow) SinceCrawled() string {
	return common.Since(&r.CrawledAt)
}

func (r HistoryListRow) NetworkIDStr() string {
	if r.NetworkID == nil {
		return ""
	}

	return fmt.Sprintf("%s (%d)", NetworkName(r.NetworkID), *r.NetworkID)
}

type HistoryList struct {
	Rows      []HistoryListRow
	NetworkID int64
	IsError   int
	Before    *time.Time
	After     *time.Time
	LastTime  *time.Time
	FirstTime *time.Time
}

var DateTimeLocal = "2006-01-02T15:04:05"

func (l HistoryList) BeforeStr() string {
	if l.Before == nil {
		return ""
	}

	return l.Before.UTC().Format(DateTimeLocal)
}

func (l HistoryList) AfterStr() string {
	if l.After == nil {
		return ""
	}

	return l.After.UTC().Format(DateTimeLocal)
}

func (l HistoryList) FirstTimeStr() string {
	if l.FirstTime == nil {
		return l.AfterStr()
	}

	return l.FirstTime.UTC().Format(DateTimeLocal)
}

func (l HistoryList) LastTimeStr() string {
	if l.FirstTime == nil {
		return l.BeforeStr()
	}

	return l.LastTime.UTC().Format(DateTimeLocal)
}

func timePtrToUnixPtr(t *time.Time) *int64 {
	if t == nil {
		return nil
	}

	u := t.Unix()
	return &u
}

func (db *DB) GetHistoryList(
	ctx context.Context,
	before *time.Time,
	after *time.Time,
	networkID int64,
	isError int,
) (*HistoryList, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_history_list", start, err)

	queryOrderDirection := "ASC"
	if before == nil {
		queryOrderDirection = "DESC"
	}

	rows, err := db.pg.Query(
		ctx,
		// Don't ever do this, but we have no other choice because I could not
		// find another way to conditionally set the order direction. :(
		fmt.Sprintf(`
			SELECT
				history.node_id,
				client_identifier,
				crawled.network_id,
				history.crawled_at,
				history.direction,
				history.error
			FROM crawler.history
			LEFT JOIN execution.nodes AS crawled USING (node_id)
			LEFT JOIN client.identifiers USING (client_identifier_id)
			WHERE
				(
					@before::TIMESTAMPTZ IS NULL
					OR history.crawled_at >= @before
				)
				AND (
					@after::TIMESTAMPTZ IS NULL
					OR history.crawled_at <= @after
				)
				AND (
					@network_id = -1
					OR crawled.network_id = @network_id
				)
				AND (
					CASE
						WHEN @error = -1 THEN
							TRUE
						WHEN @error = 0 THEN
							history.error IS NULL
						ELSE
							history.error IS NOT NULL
					END
				)
			ORDER BY history.crawled_at %s
			LIMIT 50
		`, queryOrderDirection),
		pgx.NamedArgs{
			"before":     before,
			"after":      after,
			"network_id": networkID,
			"error":      isError,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	historyList := HistoryList{
		Rows:      []HistoryListRow{},
		NetworkID: networkID,
		IsError:   isError,
		Before:    before,
		After:     after,
		FirstTime: nil,
		LastTime:  nil,
	}

	for rows.Next() {
		var row HistoryListRow
		nodeIDBytes := make([]byte, 32)

		err := rows.Scan(
			&nodeIDBytes,
			&row.ClientIdentifier,
			&row.NetworkID,
			&row.CrawledAt,
			&row.Direction,
			&row.Error,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		row.NodeID = hex.EncodeToString(nodeIDBytes[:])

		historyList.Rows = append(historyList.Rows, row)
	}

	if len(historyList.Rows) > 0 {
		slices.SortStableFunc(historyList.Rows, func(a, b HistoryListRow) int {
			if a.CrawledAt.Equal(b.CrawledAt) {
				return 0
			}
			if a.CrawledAt.After(b.CrawledAt) {
				return -1
			}
			return 1
		})

		historyList.FirstTime = &historyList.Rows[0].CrawledAt
		historyList.LastTime = &historyList.Rows[len(historyList.Rows)-1].CrawledAt
	}

	return &historyList, nil
}
