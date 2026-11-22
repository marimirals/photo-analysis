// worker: читает задачи из Kafka (photos.uploaded), вызывает analyzer по gRPC,
// ищет дубликаты и сохраняет результат в Postgres. Упавшие задачи — в photos.failed.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/marimirals/photo-analysis/internal/config"
	"github.com/marimirals/photo-analysis/internal/logx"
	"github.com/marimirals/photo-analysis/internal/store"
	"github.com/marimirals/photo-analysis/internal/worker"
)

func main() {
	log := logx.New(os.Stdout, "worker")
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("service stopped with error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	an, err := worker.NewAnalyzerClient(cfg.AnalyzerAddr, cfg.AnalyzerTimeout)
	if err != nil {
		return err
	}
	defer an.Close()

	w, err := worker.New(worker.Config{
		Brokers:              cfg.KafkaBrokers,
		Group:                cfg.KafkaGroup,
		TopicUploaded:        cfg.KafkaTopicUploaded,
		TopicFailed:          cfg.KafkaTopicFailed,
		Concurrency:          cfg.WorkerConcurrency,
		MaxAttempts:          cfg.MaxAttempts,
		DuplicateMaxDistance: cfg.DuplicateMaxDistance,
	}, st, an, log)
	if err != nil {
		return err
	}
	defer w.Close()

	// Маленький HTTP-сервер для проб Kubernetes (а на шаге 7 — для метрик).
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, r *http.Request) { rw.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(rw http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		checks, code := map[string]string{"postgres": "ok", "kafka": "ok"}, http.StatusOK
		if err := st.Ping(c); err != nil {
			checks["postgres"], code = err.Error(), http.StatusServiceUnavailable
		}
		if err := w.Ping(c); err != nil {
			checks["kafka"], code = err.Error(), http.StatusServiceUnavailable
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(code)
		_ = json.NewEncoder(rw).Encode(checks)
	})
	// Порт занимаем сразу: если он занят (например, где-то ещё работает старый worker),
	// лучше остановиться с понятной ошибкой, чем работать без метрик и проб.
	ln, err := net.Listen("tcp", cfg.WorkerHTTPAddr)
	if err != nil {
		return fmt.Errorf("listen %s (maybe another worker is running?): %w", cfg.WorkerHTTPAddr, err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
		}
	}()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()

	log.Info("starting", "analyzer", cfg.AnalyzerAddr, "http", cfg.WorkerHTTPAddr)
	err = w.Run(ctx)
	log.Info("stopped")
	return err
}
