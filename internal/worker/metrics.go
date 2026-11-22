package worker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	jobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "worker_jobs_total",
		Help: "Обработанные задачи: done — готово, failed — упало, skipped — пропущено (удалено или уже готово).",
	}, []string{"result"})

	jobDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_job_duration_seconds",
		Help:    "Время обработки одного фото целиком, вместе с ожиданием analyzer'а и записью в базу.",
		Buckets: []float64{.1, .25, .5, 1, 2, 5, 10, 30, 60, 120},
	})

	jobsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "worker_jobs_in_flight",
		Help: "Фото в работе прямо сейчас.",
	})

	analyzerCalls = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "worker_analyzer_call_duration_seconds",
		Help:    "Время одного вызова analyzer по gRPC, по коду ответа.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2, 5, 10, 30},
	}, []string{"code"})

	analyzerWaits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "worker_analyzer_unavailable_total",
		Help: "Сколько раз worker ждал недоступный analyzer.",
	})

	badShots = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "worker_bad_shots_total",
		Help: "Неудачные кадры по причинам (у одного фото может быть несколько причин).",
	}, []string{"reason"})

	duplicatesFound = promauto.NewCounter(prometheus.CounterOpts{
		Name: "worker_duplicates_found_total",
		Help: "Найденные пары дубликатов.",
	})
)
