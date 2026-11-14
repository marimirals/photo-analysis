// api-service: принимает фото, сохраняет файл в S3, запись — в Postgres,
// кладёт задачу на анализ в outbox, а фоновая горутина переносит её в Kafka.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marimirals/photo-analysis/internal/api"
	"github.com/marimirals/photo-analysis/internal/config"
	"github.com/marimirals/photo-analysis/internal/kafka"
	"github.com/marimirals/photo-analysis/internal/outbox"
	"github.com/marimirals/photo-analysis/internal/storage"
	"github.com/marimirals/photo-analysis/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "api")
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("service stopped with error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg := config.Load()

	// Ctrl+C или SIGTERM от Kubernetes — аккуратно останавливаемся.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("applying migrations")
	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return err
	}

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	s3, err := storage.NewS3(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3UseSSL)
	if err != nil {
		return err
	}
	if err := s3.EnsureBucket(ctx); err != nil {
		return err
	}

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Фоновая отправка outbox → Kafka. Остановится вместе с ctx.
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		outbox.NewRelay(st, producer.SendOutbox, log).Run(ctx)
	}()
	// Перед закрытием базы и Kafka остановить relay и дождаться его.
	defer func() { stop(); <-relayDone }()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(st, s3, producer.Ping, log, cfg.KafkaTopicUploaded, cfg.MaxUploadBytes).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
