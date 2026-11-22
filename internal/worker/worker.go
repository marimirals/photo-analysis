// Package worker разбирает очередь photos.uploaded: зовёт analyzer,
// ищет дубликаты, сохраняет результат.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	analyzerv1 "github.com/marimirals/photo-analysis/gen/analyzer/v1"
	"github.com/marimirals/photo-analysis/internal/store"
)

type Config struct {
	Brokers              string
	Group                string
	TopicUploaded        string
	TopicFailed          string
	Concurrency          int
	MaxAttempts          int
	DuplicateMaxDistance int
}

type Worker struct {
	cfg      Config
	kafka    *kgo.Client
	store    *store.Store
	analyzer *AnalyzerClient
	log      *slog.Logger
}

func New(cfg Config, st *store.Store, an *AnalyzerClient, log *slog.Logger) (*Worker, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.Brokers, ",")...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.TopicUploaded),
		// Offset коммитим сами и только после того, как вся пачка обработана.
		kgo.DisableAutoCommit(),
		// Пока пачка в работе, Kafka не отберёт у нас партиции (ребалансировка подождёт).
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Worker{cfg: cfg, kafka: cl, store: st, analyzer: an, log: log}, nil
}

func (w *Worker) Close() { w.kafka.Close() }

func (w *Worker) Ping(ctx context.Context) error { return w.kafka.Ping(ctx) }

// Run читает Kafka, пока не отменён ctx.
//
// Как устроена параллельность: пачка сообщений раскладывается по Concurrency
// горутинам по хешу user_id. Фото одного пользователя всегда попадают в одну
// горутину и обрабатываются по порядку — иначе два его фото могли бы
// одновременно искать дубликаты друг у друга и не найти. Разные пользователи
// обрабатываются параллельно. Когда вся пачка готова — коммитим offset.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker started", "group", w.cfg.Group, "topic", w.cfg.TopicUploaded, "concurrency", w.cfg.Concurrency)
	for {
		fetches := w.kafka.PollRecords(ctx, 100)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			w.log.Warn("kafka fetch error", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
		}
		records := fetches.Records()
		if len(records) == 0 {
			w.kafka.AllowRebalance()
			continue
		}

		w.processBatch(ctx, records)

		if ctx.Err() != nil {
			// Остановка посреди пачки: offset не коммитим, после перезапуска
			// пачка придёт снова. Уже сохранённые результаты перезапишутся теми же.
			w.kafka.AllowRebalance()
			return nil
		}
		if err := w.kafka.CommitRecords(ctx, records...); err != nil {
			w.log.Error("commit offsets failed", "err", err)
		}
		w.kafka.AllowRebalance()
	}
}

func (w *Worker) processBatch(ctx context.Context, records []*kgo.Record) {
	n := max(w.cfg.Concurrency, 1)
	lanes := make([][]*kgo.Record, n)
	for _, r := range records {
		h := fnv.New32a()
		h.Write(r.Key)
		i := int(h.Sum32() % uint32(n))
		lanes[i] = append(lanes[i], r)
	}

	var wg sync.WaitGroup
	for _, lane := range lanes {
		if len(lane) == 0 {
			continue
		}
		wg.Add(1)
		go func(lane []*kgo.Record) {
			defer wg.Done()
			for _, r := range lane {
				if ctx.Err() != nil {
					return
				}
				w.handle(ctx, r)
			}
		}(lane)
	}
	wg.Wait()
}

type uploadedEvent struct {
	EventID   string `json:"event_id"`
	PhotoID   string `json:"photo_id"`
	UserID    string `json:"user_id"`
	Bucket    string `json:"bucket"`
	ObjectKey string `json:"object_key"`
}

// handle обрабатывает одно сообщение. Ошибки не возвращает: всё, что не
// получилось, заканчивается статусом failed и сообщением в photos.failed.
func (w *Worker) handle(ctx context.Context, r *kgo.Record) {
	start := time.Now()
	jobsInFlight.Inc()
	defer jobsInFlight.Dec()
	result := "failed"
	defer func() {
		if ctx.Err() != nil && result == "failed" {
			return // остановка, а не настоящая ошибка — не считаем
		}
		jobsTotal.WithLabelValues(result).Inc()
		jobDuration.Observe(time.Since(start).Seconds())
	}()
	var ev uploadedEvent
	if err := json.Unmarshal(r.Value, &ev); err != nil || ev.PhotoID == "" || ev.ObjectKey == "" {
		w.log.Error("bad message, sending to DLQ", "err", err, "offset", r.Offset, "partition", r.Partition)
		w.toDLQ(ctx, r, "bad message", 0)
		return
	}
	log := w.log.With("photo_id", ev.PhotoID, "user_id", ev.UserID)

	p, err := w.store.GetPhotoByID(ctx, ev.PhotoID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		result = "skipped"
		log.Info("photo was deleted, skipping")
		return
	case err != nil:
		log.Error("load photo failed", "err", err)
		w.fail(ctx, r, ev, log, "database error: "+err.Error(), 0)
		return
	case p.Status == "done":
		result = "skipped"
		log.Info("already analyzed, skipping") // повторная доставка
		return
	}

	policy := retryPolicy{maxAttempts: w.cfg.MaxAttempts, base: 2 * time.Second, maxWait: 30 * time.Second}
	hooks := retryHooks{
		beforeAttempt: func(int) {
			if err := w.store.MarkProcessing(ctx, ev.PhotoID); err != nil && !errors.Is(err, store.ErrNotFound) {
				log.Warn("mark processing failed", "err", err)
			}
		},
		onWait: func(err error, attempt int, pause time.Duration, unavailable bool) {
			if unavailable {
				analyzerWaits.Inc()
				log.Warn("analyzer unavailable, waiting", "err", err, "retry_in", pause.String())
			} else {
				log.Warn("analysis failed, will retry", "err", err, "attempt", attempt, "retry_in", pause.String())
			}
		},
		sleep: sleepCtx,
	}
	resp, attempt, err := runWithRetry(ctx, policy, hooks,
		func(ctx context.Context) (*analyzerv1.AnalyzeResponse, error) {
			return w.analyzer.Analyze(ctx, ev.PhotoID, ev.Bucket, ev.ObjectKey)
		})
	if err != nil {
		if ctx.Err() != nil {
			return // останавливаемся; сообщение придёт снова после перезапуска
		}
		log.Warn("analysis failed", "err", err, "attempts", attempt)
		w.fail(ctx, r, ev, log, err.Error(), attempt)
		return
	}

	a := store.Analysis{
		PhotoID:         ev.PhotoID,
		BlurScore:       resp.GetBlurScore(),
		IsBlurry:        resp.GetIsBlurry(),
		FacesCount:      int(resp.GetFacesCount()),
		EyesClosedCount: int(resp.GetEyesClosedCount()),
		MeanBrightness:  resp.GetMeanBrightness(),
		IsUnderexposed:  resp.GetIsUnderexposed(),
		IsOverexposed:   resp.GetIsOverexposed(),
		Phash:           resp.GetPhash(),
		AnalyzerVersion: resp.GetAnalyzerVersion(),
	}
	a.IsBadShot = a.IsBlurry || a.EyesClosedCount > 0 || a.IsUnderexposed || a.IsOverexposed

	dups, err := w.store.SaveAnalysis(ctx, ev.UserID, a, w.cfg.DuplicateMaxDistance)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Error("save analysis failed", "err", err)
		w.fail(ctx, r, ev, log, "save failed: "+err.Error(), attempt)
		return
	}
	result = "done"
	duplicatesFound.Add(float64(len(dups)))
	for reason, bad := range map[string]bool{
		"blurry": a.IsBlurry, "eyes_closed": a.EyesClosedCount > 0,
		"underexposed": a.IsUnderexposed, "overexposed": a.IsOverexposed,
	} {
		if bad {
			badShots.WithLabelValues(reason).Inc()
		}
	}
	log.Info("photo analyzed",
		"duration_ms", time.Since(start).Milliseconds(),
		"attempts", attempt,
		"blurry", a.IsBlurry,
		"faces", a.FacesCount,
		"eyes_closed", a.EyesClosedCount,
		"bad_shot", a.IsBadShot,
		"duplicates", len(dups),
	)
}

func (w *Worker) fail(ctx context.Context, r *kgo.Record, ev uploadedEvent, log *slog.Logger, reason string, attempts int) {
	if err := w.store.MarkFailed(ctx, ev.PhotoID, reason); err != nil {
		log.Error("mark failed failed", "err", err)
	}
	w.toDLQ(ctx, r, reason, attempts)
}

// toDLQ кладёт исходное сообщение в photos.failed вместе с причиной —
// чтобы его можно было разобрать руками или переотправить.
func (w *Worker) toDLQ(ctx context.Context, r *kgo.Record, reason string, attempts int) {
	payload, _ := json.Marshal(map[string]any{
		"original":  json.RawMessage(validJSONOrString(r.Value)),
		"reason":    reason,
		"attempts":  attempts,
		"failed_at": time.Now().UTC(),
		"source":    map[string]any{"topic": r.Topic, "partition": r.Partition, "offset": r.Offset},
	})
	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	rec := &kgo.Record{Topic: w.cfg.TopicFailed, Key: r.Key, Value: payload}
	if err := w.kafka.ProduceSync(sendCtx, rec).FirstErr(); err != nil {
		w.log.Error("send to DLQ failed", "err", err)
	}
}

func validJSONOrString(b []byte) []byte {
	if json.Valid(b) {
		return b
	}
	s, _ := json.Marshal(string(b))
	return s
}
