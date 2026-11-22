package api

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/marimirals/photo-analysis/internal/store"
)

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP-запросы к api-service.",
	}, []string{"method", "route", "code"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "Время ответа api-service.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"method", "route"})

	photosUploaded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "photos_uploaded_total",
		Help: "Загруженные фото по типу файла.",
	}, []string{"content_type"})

	photoUploadBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "photo_upload_bytes",
		Help:    "Размер загруженных фото.",
		Buckets: prometheus.ExponentialBuckets(64<<10, 2, 10), // 64 КБ … 32 МБ
	})

	photosReanalyzed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "photos_reanalyze_queued_total",
		Help: "Фото, отправленные на повторный анализ.",
	})
)

// observeHTTP. route — шаблон маршрута ("GET /api/v1/photos/{id}"), а не
// настоящий путь: иначе каждый id фото породил бы свой ряд в Prometheus.
func observeHTTP(r *http.Request, status int, elapsed time.Duration) {
	route := r.Pattern
	if route == "" {
		route = "unmatched"
	}
	httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
	httpDuration.WithLabelValues(r.Method, route).Observe(elapsed.Seconds())
}

// dbCollector при каждом сборе метрик спрашивает базу, сколько фото в каком
// статусе и сколько событий ждут отправки в Kafka. Это и есть «очередь»
// с точки зрения пользователя.
type dbCollector struct {
	store   *store.Store
	log     *slog.Logger
	byState *prometheus.Desc
	outbox  *prometheus.Desc
}

func NewDBCollector(st *store.Store, log *slog.Logger) prometheus.Collector {
	return &dbCollector{
		store:   st,
		log:     log,
		byState: prometheus.NewDesc("photos_by_status", "Сколько фото в каждом статусе.", []string{"status"}, nil),
		outbox:  prometheus.NewDesc("outbox_pending", "События, ещё не отправленные в Kafka.", nil, nil),
	}
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.byState
	ch <- c.outbox
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	counts, err := c.store.CountByStatus(ctx)
	if err != nil {
		c.log.Warn("metrics: count by status failed", "err", err)
	} else {
		for _, st := range []string{"pending", "processing", "done", "failed"} {
			ch <- prometheus.MustNewConstMetric(c.byState, prometheus.GaugeValue, float64(counts[st]), st)
		}
	}
	if n, err := c.store.OutboxPending(ctx); err != nil {
		c.log.Warn("metrics: outbox pending failed", "err", err)
	} else {
		ch <- prometheus.MustNewConstMetric(c.outbox, prometheus.GaugeValue, float64(n))
	}
}
