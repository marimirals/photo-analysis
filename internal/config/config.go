// Package config читает настройки из переменных окружения.
// Значения по умолчанию подходят для локального запуска с docker-compose.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr    string
	DatabaseURL string

	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Bucket    string
	S3UseSSL    bool

	KafkaBrokers       string
	KafkaTopicUploaded string
	KafkaTopicFailed   string
	KafkaGroup         string

	MaxUploadBytes int64

	// worker
	WorkerHTTPAddr       string
	WorkerConcurrency    int
	AnalyzerAddr         string
	AnalyzerTimeout      time.Duration
	MaxAttempts          int
	DuplicateMaxDistance int
}

func Load() Config {
	return Config{
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		DatabaseURL: env("DATABASE_URL", "postgres://photo:photo@localhost:5433/photo?sslmode=disable"),

		S3Endpoint:  env("S3_ENDPOINT", "localhost:8333"),
		S3AccessKey: env("S3_ACCESS_KEY", "photo"),
		S3SecretKey: env("S3_SECRET_KEY", "photo12345"),
		S3Bucket:    env("S3_BUCKET", "photos"),
		S3UseSSL:    env("S3_USE_SSL", "false") == "true",

		KafkaBrokers:       env("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopicUploaded: env("KAFKA_TOPIC_UPLOADED", "photos.uploaded"),

		KafkaTopicFailed: env("KAFKA_TOPIC_FAILED", "photos.failed"),
		KafkaGroup:       env("KAFKA_GROUP", "photo-workers"),

		MaxUploadBytes: int64(envInt("MAX_UPLOAD_MB", 20)) << 20,

		WorkerHTTPAddr:       env("WORKER_HTTP_ADDR", ":8081"),
		WorkerConcurrency:    envInt("WORKER_CONCURRENCY", 4),
		AnalyzerAddr:         env("ANALYZER_ADDR", "localhost:50051"),
		AnalyzerTimeout:      time.Duration(envInt("ANALYZER_TIMEOUT_SEC", 30)) * time.Second,
		MaxAttempts:          envInt("MAX_ATTEMPTS", 3),
		DuplicateMaxDistance: envIntZero("DUPLICATE_MAX_DISTANCE", 6),
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

// envIntZero — как envInt, но 0 тоже допустимое значение.
func envIntZero(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v >= 0 {
		return v
	}
	return def
}
