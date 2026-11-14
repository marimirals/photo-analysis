// Package config читает настройки из переменных окружения.
// Значения по умолчанию подходят для локального запуска с docker-compose.
package config

import (
	"os"
	"strconv"
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

	MaxUploadBytes int64
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

		MaxUploadBytes: int64(envInt("MAX_UPLOAD_MB", 20)) << 20,
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
