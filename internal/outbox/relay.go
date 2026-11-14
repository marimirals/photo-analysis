// Package outbox — фоновая горутина, которая переносит события из таблицы outbox в Kafka.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/marimirals/photo-analysis/internal/store"
)

type Sender func(context.Context, []store.OutboxRecord) error

type Relay struct {
	store     *store.Store
	send      Sender
	log       *slog.Logger
	interval  time.Duration
	batchSize int
}

func NewRelay(st *store.Store, send Sender, log *slog.Logger) *Relay {
	return &Relay{store: st, send: send, log: log, interval: 500 * time.Millisecond, batchSize: 100}
}

// Run работает, пока не отменён ctx.
// Пока есть что отправлять — отправляет пачками без пауз.
// Если Kafka недоступна — ждёт всё дольше (до 30 с), события спокойно лежат в базе.
func (r *Relay) Run(ctx context.Context) {
	r.log.Info("outbox relay started")
	defer r.log.Info("outbox relay stopped")

	backoff := r.interval
	lastCleanup := time.Now()

	for {
		n, err := r.store.RelayOutbox(ctx, r.batchSize, r.send)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			r.log.Warn("outbox relay failed, will retry", "err", err, "retry_in", backoff.String())
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		case n > 0:
			r.log.Info("outbox events sent", "count", n)
			backoff = r.interval
			if n == r.batchSize {
				continue // наверняка есть ещё — сразу следующая пачка
			}
		default:
			backoff = r.interval
		}

		if time.Since(lastCleanup) > time.Hour {
			if deleted, err := r.store.CleanupOutbox(ctx, 24*time.Hour); err != nil {
				r.log.Warn("outbox cleanup failed", "err", err)
			} else if deleted > 0 {
				r.log.Info("outbox cleaned up", "deleted", deleted)
			}
			lastCleanup = time.Now()
		}

		if !sleep(ctx, r.interval) {
			return
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
