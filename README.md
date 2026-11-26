# Photo Analysis Service

A backend service for a cloud photo storage. It accepts photos, stores them and analyzes them asynchronously. For each photo it reports:

- blur;
- number of faces;
- closed eyes;
- under- and overexposure;
- duplicates among the user's photos.

Case project for the VK internship program.

## Architecture

```
client ──HTTP──▶ api ──▶ PostgreSQL (photos, results, outbox)
                  │  └─▶ S3 storage (SeaweedFS)
                  ▼
            outbox relay ──▶ Kafka (photos.uploaded)
                                  │
                                  ▼
                               worker ──gRPC──▶ analyzer (Python) ──▶ S3
                                  │
                                  ├─▶ PostgreSQL (results, duplicates)
                                  └─▶ Kafka (photos.failed, dead letter queue)
```

- **api** (Go) — REST API. Stores the file in S3 and writes the photo and an outbox event to PostgreSQL in one transaction.
- **outbox relay** — moves events from PostgreSQL to Kafka, so an uploaded photo cannot lose its analysis task.
- **worker** (Go) — Kafka consumer group. Calls the analyzer over gRPC, saves results idempotently, finds duplicates.
- **analyzer** (Python) — gRPC service that downloads the photo from S3 and runs computer vision models.

## Key decisions

- **Transactional outbox** — no lost tasks between the database write and Kafka.
- **At-least-once delivery + idempotent upsert.** Offsets are committed only after results are saved, so redelivered messages are safe.
- **Kafka key = `user_id`.** One user's photos are processed in order, so duplicate detection does not race with itself.
- **Retry policy by gRPC status code:**
  - `UNAVAILABLE` — wait for the analyzer without spending attempts;
  - `INTERNAL` — up to 3 attempts, then the dead letter queue;
  - `INVALID_ARGUMENT` / `NOT_FOUND` — fail immediately.
- **gRPC client-side load balancing** (`round_robin` + headless Service in Kubernetes). gRPC keeps one long-lived HTTP/2 connection, so a regular Service would send all requests to one analyzer pod.

## How photos are analyzed

| Check | Method | Default threshold |
|---|---|---|
| Blur | Variance of the Laplacian on a 4×4 grid, sharpest cell (portraits with blurred background are not flagged) | < 50 |
| Faces | OpenCV YuNet face detector | score ≥ 0.6 |
| Closed eyes | MediaPipe FaceLandmarker blendshapes (`eyeBlink`) on each face crop | ≥ 0.5 |
| Exposure | Mean brightness | < 40 or > 215 |
| Duplicates | 64-bit perceptual hash (DCT), Hamming distance | ≤ 6 |

Thresholds were calibrated on a set of 41 photos and can be changed with environment variables.

## Tech stack

| Area | Technologies |
|---|---|
| Services | Go 1.24+ (`net/http`, `log/slog`), Python 3.12 |
| Storage | PostgreSQL 17 (pgx, goose migrations), SeaweedFS (S3 API, minio-go) |
| Messaging | Apache Kafka 4.1 (KRaft), franz-go |
| RPC | gRPC, Protocol Buffers, gRPC health checking |
| Computer vision | OpenCV (YuNet), MediaPipe FaceLandmarker, NumPy |
| Observability | Structured JSON logs with request IDs, Prometheus, Grafana (provisioned dashboard) |
| API docs | OpenAPI 3, Swagger UI |
| Deployment | Docker (multi-stage, distroless), Kubernetes (kind, kustomize), Docker Compose |

## Run in Kubernetes (kind)

Requirements: Docker, kind, kubectl, Go 1.24+, PowerShell.

```powershell
.\deploy\k8s\up.ps1
```

The script creates a kind cluster, builds the three images, loads all images into the cluster and applies the manifests. The first run takes 10–20 minutes, mostly installing Python dependencies for the analyzer image.

| URL | What |
|---|---|
| http://localhost:30080/docs | Swagger UI |
| http://localhost:30300 | Grafana dashboard |
| http://localhost:30090 | Prometheus |

Scale the worker:

```bash
kubectl -n photo scale deployment worker --replicas=3
```

Stop: `.\deploy\k8s\down.ps1`; delete the cluster: `.\deploy\k8s\down.ps1 -Cluster`.

## Run locally (Docker Compose)

```bash
docker compose --profile grafana up -d     # PostgreSQL, SeaweedFS, Kafka, Prometheus, Grafana
```

Analyzer:

```bash
cd analyzer
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
python download_models.py    # if analyzer/models is empty
python gen_proto.py
python -m photoanalyzer.server
```

API and worker, each in its own terminal:

```bash
go run ./cmd/api
go run ./cmd/worker
```

| URL | What |
|---|---|
| http://localhost:8080/docs | Swagger UI |
| http://localhost:3000 | Grafana |
| http://localhost:9090 | Prometheus |
| http://localhost:8888/buckets/ | Stored files |

Default settings are in `.env.example`.

## API

All requests require the `X-User-ID` header (a simplification instead of authentication).

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/photos` | Upload a photo (multipart, field `file`; JPEG, PNG, WebP up to 20 MB) |
| `GET` | `/api/v1/photos` | List photos; filters `status`, `blurry`, `bad_shot`, `duplicates`, pagination |
| `GET` | `/api/v1/photos/{id}` | Photo metadata and status |
| `GET` | `/api/v1/photos/{id}/file` | Download the original |
| `GET` | `/api/v1/photos/{id}/analysis` | Analysis result (`202` while processing) |
| `POST` | `/api/v1/photos/{id}/reanalyze` | Analyze again |
| `POST` | `/api/v1/photos/reanalyze-failed` | Re-queue all failed photos |
| `DELETE` | `/api/v1/photos/{id}` | Delete a photo |
| `GET` | `/healthz`, `/readyz`, `/metrics` | Health checks and Prometheus metrics |

Example:

```bash
curl -X POST http://localhost:8080/api/v1/photos -H "X-User-ID: alice" -F "file=@photo.jpg"
curl http://localhost:8080/api/v1/photos/<id>/analysis -H "X-User-ID: alice"
```

```json
{
  "blur": { "score": 84.71, "is_blurry": false },
  "faces": { "count": 2, "eyes_closed_count": 0 },
  "exposure": { "mean_brightness": 91.07, "is_underexposed": false, "is_overexposed": false },
  "duplicate": { "phash": "9ac9c266cc2ce53d", "has_duplicates": false },
  "is_bad_shot": false,
  "bad_shot_reasons": []
}
```

## Project structure

```
cmd/api, cmd/worker      entry points
internal/api             HTTP handlers, metrics, Swagger UI
internal/store           PostgreSQL queries
internal/outbox          outbox relay
internal/worker          Kafka consumer, retries, gRPC client
api/openapi.yaml         API specification
proto/                   gRPC contract
analyzer/                Python analyzer, tests, threshold calibration script
migrations/              database schema
deploy/                  Dockerfiles, Kubernetes manifests, Prometheus and Grafana configs
```

## Tests

```bash
go test ./...
cd analyzer && python -m pytest
```
