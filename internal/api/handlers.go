// Package api — HTTP-обработчики REST API (описание в api/openapi.yaml).
package api

import (
	"bytes"
	"context"
	"errors"
	"image"
	_ "image/jpeg" // регистрируем декодеры, чтобы узнавать размер картинки
	_ "image/png"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	_ "golang.org/x/image/webp"

	"github.com/marimirals/photo-analysis/internal/storage"
	"github.com/marimirals/photo-analysis/internal/store"
)

var allowedTypes = map[string]bool{
	"image/jpeg": true,
	"image/png":  true,
	"image/webp": true,
}

// user id попадает в путь файла, поэтому разрешаем только безопасные символы.
var userIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type Server struct {
	store          *store.Store
	s3             *storage.S3
	log            *slog.Logger
	kafkaPing      func(context.Context) error
	topicUploaded  string
	maxUploadBytes int64
}

func NewServer(st *store.Store, s3 *storage.S3, kafkaPing func(context.Context) error, log *slog.Logger,
	topicUploaded string, maxUploadBytes int64) *Server {
	return &Server{store: st, s3: s3, kafkaPing: kafkaPing, log: log, topicUploaded: topicUploaded, maxUploadBytes: maxUploadBytes}
}

// Routes собирает все маршруты. Шаблоны вида "POST /path/{id}" — встроенный роутер Go 1.22+.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/photos", s.withUser(s.uploadPhoto))
	mux.HandleFunc("GET /api/v1/photos", s.withUser(s.listPhotos))
	mux.HandleFunc("GET /api/v1/photos/{id}", s.withUser(s.withPhotoID(s.getPhoto)))
	mux.HandleFunc("DELETE /api/v1/photos/{id}", s.withUser(s.withPhotoID(s.deletePhoto)))
	mux.HandleFunc("GET /api/v1/photos/{id}/file", s.withUser(s.withPhotoID(s.getPhotoFile)))

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.ready)

	return requestID(logRequests(s.log, mux))
}

// ---------- загрузка ----------

func (s *Server) uploadPhoto(w http.ResponseWriter, r *http.Request, userID string) {
	// +1 МБ на служебные части multipart-запроса
	r.Body = http.MaxBytesReader(w, r.Body, s.maxUploadBytes+1<<20)

	file, header, err := r.FormFile("file")
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, r, http.StatusRequestEntityTooLarge, "file is too large")
			return
		}
		writeError(w, r, http.StatusBadRequest, `expected multipart form with field "file"`)
		return
	}
	defer file.Close()

	if header.Size > s.maxUploadBytes {
		writeError(w, r, http.StatusRequestEntityTooLarge, "file is too large")
		return
	}
	if header.Size == 0 {
		writeError(w, r, http.StatusBadRequest, "file is empty")
		return
	}

	// Тип определяем по содержимому, а не по расширению или заголовку клиента.
	contentType, err := sniffType(file)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "cannot read file")
		return
	}
	if !allowedTypes[contentType] {
		writeError(w, r, http.StatusUnsupportedMediaType, "only JPEG, PNG and WebP are supported, got "+contentType)
		return
	}

	// Размеры картинки: заодно проверяем, что это действительно читаемое изображение.
	cfg, _, err := image.DecodeConfig(file)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "file is not a valid image")
		return
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		writeError(w, r, http.StatusInternalServerError, "internal error")
		return
	}

	photoID := newUUID()
	p := &store.Photo{
		ID:          photoID,
		UserID:      userID,
		Filename:    filepath.Base(header.Filename),
		ContentType: contentType,
		SizeBytes:   header.Size,
		Width:       &cfg.Width,
		Height:      &cfg.Height,
		ObjectKey:   userID + "/" + photoID,
	}

	ctx := r.Context()

	// Сначала файл, потом запись в базе: если упадём между ними, останется
	// лишний файл без записи, но не запись без файла.
	if err := s.s3.Put(ctx, p.ObjectKey, file, p.SizeBytes, contentType); err != nil {
		s.log.ErrorContext(ctx, "upload to s3 failed", "err", err, "photo_id", photoID)
		writeError(w, r, http.StatusBadGateway, "storage is unavailable")
		return
	}

	ev := store.UploadedEvent{
		EventID:    newUUID(),
		PhotoID:    photoID,
		UserID:     userID,
		Bucket:     s.s3.Bucket(),
		ObjectKey:  p.ObjectKey,
		UploadedAt: time.Now().UTC(),
	}
	if err := s.store.CreatePhoto(ctx, p, s.topicUploaded, ev); err != nil {
		s.log.ErrorContext(ctx, "save photo failed", "err", err, "photo_id", photoID)
		// Подчищаем файл, чтобы не копились сироты. Контекст новый: запрос мог уже отмениться.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if delErr := s.s3.Delete(cleanupCtx, p.ObjectKey); delErr != nil {
			s.log.Warn("cleanup s3 object failed", "err", delErr, "photo_id", photoID)
		}
		writeError(w, r, http.StatusInternalServerError, "cannot save photo")
		return
	}

	s.log.InfoContext(ctx, "photo uploaded", "photo_id", photoID, "user_id", userID,
		"size_bytes", p.SizeBytes, "content_type", contentType)
	writeJSON(w, http.StatusAccepted, p)
}

func sniffType(f multipart.File) (string, error) {
	buf := make([]byte, 512)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return "", err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return http.DetectContentType(buf[:n]), nil
}

// ---------- чтение ----------

func (s *Server) getPhoto(w http.ResponseWriter, r *http.Request, userID, id string) {
	p, err := s.store.GetPhoto(r.Context(), userID, id)
	if s.handleStoreErr(w, r, err) {
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) listPhotos(w http.ResponseWriter, r *http.Request, userID string) {
	q := r.URL.Query()
	f := store.ListFilter{
		Status: q.Get("status"),
		Limit:  queryInt(q.Get("limit"), 20, 1, 100),
		Offset: queryInt(q.Get("offset"), 0, 0, 1_000_000),
	}
	switch f.Status {
	case "", "pending", "processing", "done", "failed":
	default:
		writeError(w, r, http.StatusBadRequest, "unknown status")
		return
	}

	photos, total, err := s.store.ListPhotos(r.Context(), userID, f)
	if s.handleStoreErr(w, r, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": photos, "total": total})
}

func (s *Server) getPhotoFile(w http.ResponseWriter, r *http.Request, userID, id string) {
	p, err := s.store.GetPhoto(r.Context(), userID, id)
	if s.handleStoreErr(w, r, err) {
		return
	}
	obj, err := s.s3.Get(r.Context(), p.ObjectKey)
	if err != nil {
		s.log.ErrorContext(r.Context(), "get from s3 failed", "err", err, "photo_id", id)
		writeError(w, r, http.StatusBadGateway, "storage is unavailable")
		return
	}
	defer obj.Close()

	w.Header().Set("Content-Type", p.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(p.SizeBytes, 10))
	w.Header().Set("Content-Disposition", `inline; filename="`+safeFilename(p.Filename)+`"`)
	if _, err := io.Copy(w, obj); err != nil {
		s.log.WarnContext(r.Context(), "stream file interrupted", "err", err, "photo_id", id)
	}
}

func (s *Server) deletePhoto(w http.ResponseWriter, r *http.Request, userID, id string) {
	key, err := s.store.DeletePhoto(r.Context(), userID, id)
	if s.handleStoreErr(w, r, err) {
		return
	}
	if err := s.s3.Delete(r.Context(), key); err != nil {
		// Запись уже удалена, файл подчистим позже — это не ошибка для клиента.
		s.log.WarnContext(r.Context(), "delete from s3 failed", "err", err, "photo_id", id)
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------- служебное ----------

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	// Kafka в readyz не влияет на статус: без неё загрузка всё равно работает,
	// задачи подождут в outbox. Показываем только для информации.
	checks := map[string]string{"postgres": "ok", "s3": "ok", "kafka": "ok"}
	status := http.StatusOK
	if err := s.store.Ping(ctx); err != nil {
		checks["postgres"] = err.Error()
		status = http.StatusServiceUnavailable
	}
	if err := s.s3.Ping(ctx); err != nil {
		checks["s3"] = err.Error()
		status = http.StatusServiceUnavailable
	}
	if err := s.kafkaPing(ctx); err != nil {
		checks["kafka"] = err.Error()
	}
	writeJSON(w, status, checks)
}

// withUser достаёт пользователя из заголовка X-User-ID (упрощение вместо авторизации).
func (s *Server) withUser(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get("X-User-ID")
		if !userIDRe.MatchString(userID) {
			writeError(w, r, http.StatusBadRequest, "header X-User-ID is required: letters, digits, '-' or '_', up to 64 chars")
			return
		}
		next(w, r, userID)
	}
}

func (s *Server) withPhotoID(next func(http.ResponseWriter, *http.Request, string, string)) func(http.ResponseWriter, *http.Request, string) {
	return func(w http.ResponseWriter, r *http.Request, userID string) {
		id := r.PathValue("id")
		if !uuidRe.MatchString(id) {
			writeError(w, r, http.StatusNotFound, "photo not found")
			return
		}
		next(w, r, userID, id)
	}
}

// handleStoreErr пишет ответ, если была ошибка, и возвращает true.
func (s *Server) handleStoreErr(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, store.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "photo not found")
	default:
		s.log.ErrorContext(r.Context(), "database error", "err", err)
		writeError(w, r, http.StatusInternalServerError, "internal error")
	}
	return true
}

func queryInt(v string, def, min, max int) int {
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func safeFilename(name string) string {
	var b bytes.Buffer
	for _, r := range name {
		if r == '"' || r == '\\' || r < 0x20 {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
