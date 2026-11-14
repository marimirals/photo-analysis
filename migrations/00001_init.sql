-- +goose Up
CREATE TABLE photos (
    id            UUID PRIMARY KEY,
    user_id       TEXT        NOT NULL,
    filename      TEXT        NOT NULL,
    content_type  TEXT        NOT NULL,
    size_bytes    BIGINT      NOT NULL,
    width         INT,
    height        INT,
    object_key    TEXT        NOT NULL UNIQUE,      -- ключ файла в S3
    status        TEXT        NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'processing', 'done', 'failed')),
    attempts      INT         NOT NULL DEFAULT 0,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX photos_user_created_idx ON photos (user_id, created_at DESC);

CREATE TABLE analysis (
    photo_id          UUID PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,
    blur_score        DOUBLE PRECISION NOT NULL,
    is_blurry         BOOLEAN NOT NULL,
    faces_count       INT     NOT NULL,
    eyes_closed_count INT     NOT NULL,
    mean_brightness   DOUBLE PRECISION NOT NULL,
    is_underexposed   BOOLEAN NOT NULL,
    is_overexposed    BOOLEAN NOT NULL,
    phash             BIGINT  NOT NULL,     -- uint64 хранится как int64 (те же биты)
    is_bad_shot       BOOLEAN NOT NULL,
    analyzer_version  TEXT    NOT NULL,
    analyzed_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Пары дубликатов храним один раз: photo_a < photo_b.
CREATE TABLE duplicates (
    photo_a  UUID NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    photo_b  UUID NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    distance INT  NOT NULL,                 -- расстояние Хэмминга между phash
    PRIMARY KEY (photo_a, photo_b),
    CHECK (photo_a < photo_b)
);
CREATE INDEX duplicates_b_idx ON duplicates (photo_b);

-- Outbox: событие пишется в той же транзакции, что и фото,
-- отдельная горутина отправляет его в Kafka и ставит sent_at (шаг 4).
CREATE TABLE outbox (
    id          BIGSERIAL PRIMARY KEY,
    topic       TEXT        NOT NULL,
    key         TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ
);
CREATE INDEX outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS duplicates;
DROP TABLE IF EXISTS analysis;
DROP TABLE IF EXISTS photos;
