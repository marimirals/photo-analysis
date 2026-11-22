package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/marimirals/photo-analysis/internal/store"
)

// analysisResponse — результат анализа в том виде, как он описан в api/openapi.yaml.
type analysisResponse struct {
	PhotoID string `json:"photo_id"`
	Blur    struct {
		Score    float64 `json:"score"`
		IsBlurry bool    `json:"is_blurry"`
	} `json:"blur"`
	Faces struct {
		Count           int `json:"count"`
		EyesClosedCount int `json:"eyes_closed_count"`
	} `json:"faces"`
	Exposure struct {
		MeanBrightness float64 `json:"mean_brightness"`
		IsUnderexposed bool    `json:"is_underexposed"`
		IsOverexposed  bool    `json:"is_overexposed"`
	} `json:"exposure"`
	Duplicate struct {
		Phash       string   `json:"phash"`
		DuplicateOf []string `json:"duplicate_of,omitempty"`
		HasAny      bool     `json:"has_duplicates"`
	} `json:"duplicate"`
	IsBadShot       bool      `json:"is_bad_shot"`
	BadShotReasons  []string  `json:"bad_shot_reasons"`
	AnalyzerVersion string    `json:"analyzer_version"`
	AnalyzedAt      time.Time `json:"analyzed_at"`
}

func toAnalysisResponse(a *store.Analysis, dups []string, hasDups bool) *analysisResponse {
	r := &analysisResponse{PhotoID: a.PhotoID, IsBadShot: a.IsBadShot,
		AnalyzerVersion: a.AnalyzerVersion, AnalyzedAt: a.AnalyzedAt}
	r.Blur.Score, r.Blur.IsBlurry = a.BlurScore, a.IsBlurry
	r.Faces.Count, r.Faces.EyesClosedCount = a.FacesCount, a.EyesClosedCount
	r.Exposure.MeanBrightness = a.MeanBrightness
	r.Exposure.IsUnderexposed, r.Exposure.IsOverexposed = a.IsUnderexposed, a.IsOverexposed
	r.Duplicate.Phash = fmt.Sprintf("%016x", a.Phash)
	r.Duplicate.DuplicateOf = dups
	r.Duplicate.HasAny = hasDups || len(dups) > 0

	r.BadShotReasons = []string{}
	if a.IsBlurry {
		r.BadShotReasons = append(r.BadShotReasons, "blurry")
	}
	if a.EyesClosedCount > 0 {
		r.BadShotReasons = append(r.BadShotReasons, "eyes_closed")
	}
	if a.IsUnderexposed {
		r.BadShotReasons = append(r.BadShotReasons, "underexposed")
	}
	if a.IsOverexposed {
		r.BadShotReasons = append(r.BadShotReasons, "overexposed")
	}
	return r
}

// GET /api/v1/photos/{id}/analysis
// 200 — анализ готов; 202 — ещё в очереди или в работе (в ответе фото со статусом);
// для status = failed тоже 202 с полем error: анализа нет и не будет без повторной загрузки.
func (s *Server) getAnalysis(w http.ResponseWriter, r *http.Request, userID, id string) {
	p, err := s.store.GetPhoto(r.Context(), userID, id)
	if s.handleStoreErr(w, r, err) {
		return
	}
	if p.Status != "done" {
		writeJSON(w, http.StatusAccepted, p)
		return
	}
	a, dups, err := s.store.GetAnalysis(r.Context(), userID, id)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusAccepted, p) // статус уже done, а запись ещё не видна — редкая гонка
		return
	}
	if s.handleStoreErr(w, r, err) {
		return
	}
	writeJSON(w, http.StatusOK, toAnalysisResponse(a, dups, false))
}

// photoListItem — фото в списке, с результатом анализа, если он уже есть.
type photoListItem struct {
	store.Photo
	Analysis *analysisResponse `json:"analysis"`
}

// parseBool: "" → nil (не фильтровать), "true"/"false"/"1"/"0" → значение.
func parseBool(v string) (*bool, error) {
	if v == "" {
		return nil, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (s *Server) newUploadedEvent(photoID, userID, objectKey string) store.UploadedEvent {
	return store.UploadedEvent{
		EventID:    newUUID(),
		PhotoID:    photoID,
		UserID:     userID,
		Bucket:     s.s3.Bucket(),
		ObjectKey:  objectKey,
		UploadedAt: time.Now().UTC(),
	}
}

// POST /api/v1/photos/{id}/reanalyze — проанализировать фото ещё раз
// (например, после ошибки или после смены порогов). Только для done и failed.
func (s *Server) reanalyzePhoto(w http.ResponseWriter, r *http.Request, userID, id string) {
	p, err := s.store.Reanalyze(r.Context(), userID, id, s.topicUploaded, func(key string) store.UploadedEvent {
		return s.newUploadedEvent(id, userID, key)
	})
	if errors.Is(err, store.ErrConflict) {
		writeError(w, r, http.StatusConflict, "photo is already queued or being analyzed")
		return
	}
	if s.handleStoreErr(w, r, err) {
		return
	}
	photosReanalyzed.Inc()
	s.log.InfoContext(r.Context(), "photo queued for reanalysis", "photo_id", id, "user_id", userID)
	writeJSON(w, http.StatusAccepted, p)
}

// POST /api/v1/photos/reanalyze-failed — отправить на анализ все упавшие фото пользователя.
func (s *Server) reanalyzeFailed(w http.ResponseWriter, r *http.Request, userID string) {
	n, err := s.store.ReanalyzeFailed(r.Context(), userID, s.topicUploaded, func(photoID, key string) store.UploadedEvent {
		return s.newUploadedEvent(photoID, userID, key)
	})
	if s.handleStoreErr(w, r, err) {
		return
	}
	photosReanalyzed.Add(float64(n))
	s.log.InfoContext(r.Context(), "failed photos queued for reanalysis", "user_id", userID, "count", n)
	writeJSON(w, http.StatusAccepted, map[string]int{"queued": n})
}
