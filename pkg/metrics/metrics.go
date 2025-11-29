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
	//nolint:promlinter
	// we're using a gauge to make a fake histogram.
	LastFound = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "last_found",
			Help:      "Number of nodes in each time bucket by last_found",
		},
		[]string{
			"le",
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
	DiscCrawlCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "disc_crawl_total",
			Help:      "Number of discovery crawled nodes",
		},
		[]string{
			"disc_version",
		},
	)
	PortalDiscCrawlCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "portal",
			Name:      "disc_crawl_total",
			Help:      "Number of portal discovery crawled nodes",
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
	DBStatsNodesToCrawl = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "database_stats",
			Name:      "nodes_to_crawl",
			Help:      "Number of nodes to crawl",
		},
	)
	DBStatsDiscNodesToCrawl = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "database_stats",
			Name:      "disc_nodes_to_crawl",
			Help:      "Number of discovery nodes to crawl",
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
