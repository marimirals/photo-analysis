package store

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/jackc/pgx/v5"
)

// Analysis — результат анализа одной фотографии.
type Analysis struct {
	PhotoID         string
	BlurScore       float64
	IsBlurry        bool
	FacesCount      int
	EyesClosedCount int
	MeanBrightness  float64
	IsUnderexposed  bool
	IsOverexposed   bool
	Phash           uint64
	IsBadShot       bool
	AnalyzerVersion string
	AnalyzedAt      time.Time
}

// GetPhotoByID — для worker'а: ищет фото без проверки пользователя.
func (s *Store) GetPhotoByID(ctx context.Context, id string) (*Photo, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+photoColumns+` FROM photos WHERE id = $1::uuid`, id)
	return scanPhoto(row)
}

// MarkProcessing ставит статус «в обработке» и увеличивает счётчик попыток.
func (s *Store) MarkProcessing(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE photos SET status = 'processing', attempts = attempts + 1, error = NULL, updated_at = now()
		WHERE id = $1::uuid`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, id, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE photos SET status = 'failed', error = $2, updated_at = now()
		WHERE id = $1::uuid`, id, reason)
	return err
}

// SaveAnalysis одной транзакцией: сохраняет результат, ищет дубликаты среди
// остальных фото того же пользователя и ставит статус «готово».
// Повторный вызов для того же фото просто перезапишет результат — так
// повторная доставка сообщения из Kafka ничего не ломает.
// Возвращает id найденных дубликатов.
func (s *Store) SaveAnalysis(ctx context.Context, userID string, a Analysis, maxDistance int) ([]string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO analysis (photo_id, blur_score, is_blurry, faces_count, eyes_closed_count,
			mean_brightness, is_underexposed, is_overexposed, phash, is_bad_shot, analyzer_version, analyzed_at)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
		ON CONFLICT (photo_id) DO UPDATE SET
			blur_score = EXCLUDED.blur_score, is_blurry = EXCLUDED.is_blurry,
			faces_count = EXCLUDED.faces_count, eyes_closed_count = EXCLUDED.eyes_closed_count,
			mean_brightness = EXCLUDED.mean_brightness, is_underexposed = EXCLUDED.is_underexposed,
			is_overexposed = EXCLUDED.is_overexposed, phash = EXCLUDED.phash,
			is_bad_shot = EXCLUDED.is_bad_shot, analyzer_version = EXCLUDED.analyzer_version,
			analyzed_at = now()`,
		a.PhotoID, a.BlurScore, a.IsBlurry, a.FacesCount, a.EyesClosedCount,
		a.MeanBrightness, a.IsUnderexposed, a.IsOverexposed, int64(a.Phash), a.IsBadShot, a.AnalyzerVersion)
	if err != nil {
		return nil, fmt.Errorf("upsert analysis: %w", err)
	}

	// Дубликаты: сравниваем хеш со всеми уже проанализированными фото пользователя.
	// Для учебного масштаба (сотни–тысячи фото) полный перебор в Go — это миллисекунды.
	rows, err := tx.Query(ctx, `
		SELECT a.photo_id::text, a.phash FROM analysis a
		JOIN photos p ON p.id = a.photo_id
		WHERE p.user_id = $1 AND a.photo_id <> $2::uuid`, userID, a.PhotoID)
	if err != nil {
		return nil, fmt.Errorf("load hashes: %w", err)
	}
	type match struct {
		id   string
		dist int
	}
	var matches []match
	for rows.Next() {
		var id string
		var h int64
		if err := rows.Scan(&id, &h); err != nil {
			rows.Close()
			return nil, err
		}
		if d := bits.OnesCount64(uint64(h) ^ a.Phash); d <= maxDistance {
			matches = append(matches, match{id, d})
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Старые пары этого фото убираем: при повторном анализе хеш мог измениться.
	if _, err := tx.Exec(ctx, `DELETE FROM duplicates WHERE photo_a = $1::uuid OR photo_b = $1::uuid`, a.PhotoID); err != nil {
		return nil, err
	}
	dupIDs := make([]string, 0, len(matches))
	for _, m := range matches {
		_, err := tx.Exec(ctx, `
			INSERT INTO duplicates (photo_a, photo_b, distance)
			VALUES (LEAST($1::uuid, $2::uuid), GREATEST($1::uuid, $2::uuid), $3)
			ON CONFLICT (photo_a, photo_b) DO UPDATE SET distance = EXCLUDED.distance`,
			a.PhotoID, m.id, m.dist)
		if err != nil {
			return nil, fmt.Errorf("insert duplicate: %w", err)
		}
		dupIDs = append(dupIDs, m.id)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE photos SET status = 'done', error = NULL, updated_at = now()
		WHERE id = $1::uuid`, a.PhotoID); err != nil {
		return nil, err
	}
	return dupIDs, tx.Commit(ctx)
}

// GetAnalysis — результат и список дубликатов для фото этого пользователя.
func (s *Store) GetAnalysis(ctx context.Context, userID, photoID string) (*Analysis, []string, error) {
	var a Analysis
	var h int64
	err := s.pool.QueryRow(ctx, `
		SELECT a.photo_id::text, a.blur_score, a.is_blurry, a.faces_count, a.eyes_closed_count,
			a.mean_brightness, a.is_underexposed, a.is_overexposed, a.phash, a.is_bad_shot,
			a.analyzer_version, a.analyzed_at
		FROM analysis a JOIN photos p ON p.id = a.photo_id
		WHERE a.photo_id = $1::uuid AND p.user_id = $2`, photoID, userID,
	).Scan(&a.PhotoID, &a.BlurScore, &a.IsBlurry, &a.FacesCount, &a.EyesClosedCount,
		&a.MeanBrightness, &a.IsUnderexposed, &a.IsOverexposed, &h, &a.IsBadShot,
		&a.AnalyzerVersion, &a.AnalyzedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	a.Phash = uint64(h)

	rows, err := s.pool.Query(ctx, `
		SELECT CASE WHEN photo_a = $1::uuid THEN photo_b ELSE photo_a END::text
		FROM duplicates WHERE photo_a = $1::uuid OR photo_b = $1::uuid
		ORDER BY distance`, photoID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	dups := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, nil, err
		}
		dups = append(dups, id)
	}
	return &a, dups, rows.Err()
}

var ErrConflict = errors.New("conflict")

// Reanalyze отправляет фото на анализ ещё раз: статус → pending, событие → outbox,
// одной транзакцией. Можно только для done и failed: если фото уже в очереди
// или в работе, второй раз ставить его незачем (ErrConflict).
// makeEvent получает object_key и возвращает событие для Kafka.
func (s *Store) Reanalyze(ctx context.Context, userID, photoID, topic string, makeEvent func(objectKey string) UploadedEvent) (*Photo, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		UPDATE photos SET status = 'pending', error = NULL, attempts = 0, updated_at = now()
		WHERE id = $1::uuid AND user_id = $2 AND status IN ('done', 'failed')
		RETURNING `+photoColumns, photoID, userID)
	p, err := scanPhoto(row)
	if errors.Is(err, ErrNotFound) {
		// Либо фото нет, либо оно уже pending/processing — различаем для понятного ответа.
		var status string
		e := tx.QueryRow(ctx, `SELECT status FROM photos WHERE id = $1::uuid AND user_id = $2`, photoID, userID).Scan(&status)
		if errors.Is(e, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		if e != nil {
			return nil, e
		}
		return nil, ErrConflict
	}
	if err != nil {
		return nil, err
	}
	if err := insertOutbox(ctx, tx, topic, makeEvent(p.ObjectKey)); err != nil {
		return nil, err
	}
	return p, tx.Commit(ctx)
}

// ReanalyzeFailed отправляет на повторный анализ все упавшие фото пользователя.
// Возвращает, сколько фото поставлено в очередь.
func (s *Store) ReanalyzeFailed(ctx context.Context, userID, topic string, makeEvent func(photoID, objectKey string) UploadedEvent) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		UPDATE photos SET status = 'pending', error = NULL, attempts = 0, updated_at = now()
		WHERE user_id = $1 AND status = 'failed'
		RETURNING id::text, object_key`, userID)
	if err != nil {
		return 0, err
	}
	type item struct{ id, key string }
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.key); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, it := range items {
		if err := insertOutbox(ctx, tx, topic, makeEvent(it.id, it.key)); err != nil {
			return 0, err
		}
	}
	return len(items), tx.Commit(ctx)
}
