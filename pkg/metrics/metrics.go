//nolint:exhaustruct
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	namespace = "node_crawler"

	dbQueryHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "db_query_seconds",
			Help:      "Seconds spent on each database query",
		},
		[]string{
			"query_name",
			"status",
		},
	)
	DiscUpdateBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "disc_update_backlog",
		Help:      "Number of discovered nodes in the backlog",
	})
	DiscUpdateCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "disc_update_total",
			Help:      "Number of discovered nodes",
		},
		[]string{
			"disc_version",
		},
	)
	nodeUpdateBacklog = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "node_update_backlog",
			Help:      "Number of nodes to be updated in the backlog",
		}, []string{
			"system",
		},
	)
	nodeUpdateCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "node_update_total",
			Help:      "Number of updated nodes",
		},
		[]string{
			"direction",
			"status",
			"error",
		},
	)
	DBStatsCrawledNodes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "database_stats",
			Name:      "crawled_nodes",
			Help:      "Number of crawled nodes in the database",
		},
	)
	DBStatsDiscNodes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "database_stats",
			Name:      "discovered_nodes",
			Help:      "Number of discovered nodes in the database",
		},
	)
	DBStatsNodesToCrawl = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "database_stats",
			Name:      "nodes_to_crawl",
			Help:      "Number of nodes to crawl",
		},
	)
	DBStatsBlocks = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "database_stats",
			Name:      "blocks",
			Help:      "Number of blocks in the database",
		},
	)
	DBStatsSizeBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "database_stats",
			Name:      "table_size_bytes",
			Help:      "Size of each table in bytes",
		},
		[]string{
			"schema",
			"table",
		},
	)
	DatabaseRetries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "database_retries_total",
			Help:      "Number of retries per retry count",
		},
		[]string{
			"retry",
		},
	)
	startTime = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "start_time",
			Help:      "Unix timestamp when the service started",
		},
	)
	AcceptedConnections = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "accepted_connections_total",
			Help:      "Number of accepted connections.",
		},
		[]string{
			"addr",
		},
	)
)

func init() {
	startTime.SetToCurrentTime()
}

func boolToStatus(b bool) string {
	if b {
		return "success"
	}

	return "error"
}

func ObserveDBQuery(queryName string, start time.Time, err error) {
	dbQueryHistogram.With(prometheus.Labels{
		"query_name": queryName,
		"status":     boolToStatus(err == nil),
	}).Observe(time.Since(start).Seconds())
}

func NodeUpdateInc(direction string, err string) {
	nodeUpdateCount.With(prometheus.Labels{
		"direction": direction,
		"status":    boolToStatus(err == ""),
		"error":     err,
	}).Inc()
}

func NodeUpdateBacklog(system string, value int) {
	nodeUpdateBacklog.WithLabelValues(system).Set(float64(value))
}
