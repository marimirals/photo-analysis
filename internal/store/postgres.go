// Package store — работа с базой: фото и outbox.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // драйвер database/sql для goose
	"github.com/pressly/goose/v3"

	"github.com/marimirals/photo-analysis/migrations"
)

var ErrNotFound = errors.New("not found")

type Photo struct {
	ID          string    `json:"id"`
	UserID      string    `json:"-"`
	Filename    string    `json:"filename"`
	ContentType string    `json:"content_type"`
	SizeBytes   int64     `json:"size_bytes"`
	Width       *int      `json:"width"`
	Height      *int      `json:"height"`
	ObjectKey   string    `json:"-"`
	Status      string    `json:"status"`
	Error       *string   `json:"error"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// UploadedEvent — сообщение для Kafka о новой фотографии.
type UploadedEvent struct {
	EventID    string    `json:"event_id"`
	PhotoID    string    `json:"photo_id"`
	UserID     string    `json:"user_id"`
	Bucket     string    `json:"bucket"`
	ObjectKey  string    `json:"object_key"`
	UploadedAt time.Time `json:"uploaded_at"`
}

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate накатывает миграции из папки migrations (они встроены в бинарник).
func Migrate(databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.Up(db, ".")
}

// CreatePhoto сохраняет фото и событие для Kafka одной транзакцией:
// либо записано и то и другое, либо ничего.
func (s *Store) CreatePhoto(ctx context.Context, p *Photo, topic string, ev UploadedEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // после Commit ничего не делает

	err = tx.QueryRow(ctx, `
		INSERT INTO photos (id, user_id, filename, content_type, size_bytes, width, height, object_key)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)
		RETURNING status, created_at, updated_at`,
		p.ID, p.UserID, p.Filename, p.ContentType, p.SizeBytes, p.Width, p.Height, p.ObjectKey,
	).Scan(&p.Status, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert photo: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
		topic, ev.UserID, payload)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	return tx.Commit(ctx)
}

const photoColumns = `id::text, user_id, filename, content_type, size_bytes, width, height,
	object_key, status, error, created_at, updated_at`

func scanPhoto(row pgx.Row) (*Photo, error) {
	var p Photo
	err := row.Scan(&p.ID, &p.UserID, &p.Filename, &p.ContentType, &p.SizeBytes, &p.Width, &p.Height,
		&p.ObjectKey, &p.Status, &p.Error, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// GetPhoto ищет фото только среди фото этого пользователя.
func (s *Store) GetPhoto(ctx context.Context, userID, id string) (*Photo, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+photoColumns+` FROM photos WHERE id = $1::uuid AND user_id = $2`, id, userID)
	return scanPhoto(row)
}

type ListFilter struct {
	Status string // пусто — любой
	Limit  int
	Offset int
}

func (s *Store) ListPhotos(ctx context.Context, userID string, f ListFilter) ([]Photo, int, error) {
	var total int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM photos WHERE user_id = $1 AND ($2 = '' OR status = $2)`,
		userID, f.Status).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT `+photoColumns+` FROM photos
		WHERE user_id = $1 AND ($2 = '' OR status = $2)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4`,
		userID, f.Status, f.Limit, f.Offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	photos := []Photo{}
	for rows.Next() {
		p, err := scanPhoto(rows)
		if err != nil {
			return nil, 0, err
		}
		photos = append(photos, *p)
	}
	return photos, total, rows.Err()
}

// DeletePhoto удаляет запись и возвращает ключ файла, чтобы удалить и его.
func (s *Store) DeletePhoto(ctx context.Context, userID, id string) (string, error) {
	var key string
	err := s.pool.QueryRow(ctx,
		`DELETE FROM photos WHERE id = $1::uuid AND user_id = $2 RETURNING object_key`,
		id, userID).Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return key, err
}
