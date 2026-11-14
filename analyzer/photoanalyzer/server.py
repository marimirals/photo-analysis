"""gRPC-сервер анализа. Запуск: python -m photoanalyzer.server (из папки analyzer)."""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from concurrent import futures
from pathlib import Path

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from minio import Minio
from minio.error import S3Error

from photoanalyzer import VERSION
from photoanalyzer.core import Analyzer, Thresholds

# Код из analyzer.proto генерирует gen_proto.py в папку gen/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gen"))
import analyzer_pb2  # noqa: E402
import analyzer_pb2_grpc  # noqa: E402

log = logging.getLogger("analyzer")


class JsonFormatter(logging.Formatter):
    """Логи в JSON, как у Go-сервисов: одна строка — одно событие."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(record.created)),
            "level": record.levelname,
            "msg": record.getMessage(),
            "service": "analyzer",
        }
        entry.update(getattr(record, "fields", {}))
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def env(name: str, default: str) -> str:
    return os.environ.get(name) or default


class AnalyzerService(analyzer_pb2_grpc.AnalyzerServicer):
    def __init__(self, analyzer: Analyzer, s3: Minio):
        self.analyzer = analyzer
        self.s3 = s3

    def _fetch(self, bucket: str, key: str) -> bytes:
        resp = self.s3.get_object(bucket, key)
        try:
            return resp.read()
        finally:
            resp.close()
            resp.release_conn()

    def Analyze(self, request, context):
        if not request.bucket or not request.object_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "bucket and object_key are required")

        fields = {"photo_id": request.photo_id, "object_key": request.object_key}
        start = time.monotonic()

        try:
            data = self._fetch(request.bucket, request.object_key)
        except S3Error as e:
            if e.code in ("NoSuchKey", "NoSuchBucket"):
                context.abort(grpc.StatusCode.NOT_FOUND, f"object not found: {request.object_key}")
            log.warning("s3 error", extra={"fields": {**fields, "err": str(e)}})
            context.abort(grpc.StatusCode.UNAVAILABLE, f"storage error: {e.code}")
        except Exception as e:  # сеть, таймаут и т.п.
            log.warning("s3 unavailable", extra={"fields": {**fields, "err": str(e)}})
            context.abort(grpc.StatusCode.UNAVAILABLE, "storage is unavailable")

        try:
            r = self.analyzer.analyze_bytes(data)
        except ValueError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except Exception:
            log.exception("analysis failed", extra={"fields": fields})
            context.abort(grpc.StatusCode.INTERNAL, "analysis failed")

        log.info("analyzed", extra={"fields": {
            **fields,
            "duration_ms": round((time.monotonic() - start) * 1000),
            "blur_score": r.blur_score,
            "faces": r.faces_count,
            "eyes_closed": r.eyes_closed_count,
            "brightness": r.mean_brightness,
        }})
        return analyzer_pb2.AnalyzeResponse(
            blur_score=r.blur_score,
            is_blurry=r.is_blurry,
            faces_count=r.faces_count,
            eyes_closed_count=r.eyes_closed_count,
            mean_brightness=r.mean_brightness,
            is_underexposed=r.is_underexposed,
            is_overexposed=r.is_overexposed,
            phash=r.phash,
            width=r.width,
            height=r.height,
            analyzer_version=VERSION,
        )


def main() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    addr = env("ANALYZER_ADDR", "[::]:50051")
    workers = int(env("ANALYZER_WORKERS", "2"))

    s3 = Minio(
        env("S3_ENDPOINT", "localhost:8333"),
        access_key=env("S3_ACCESS_KEY", "photo"),
        secret_key=env("S3_SECRET_KEY", "photo12345"),
        secure=env("S3_USE_SSL", "false") == "true",
    )
    thresholds = Thresholds()
    analyzer = Analyzer(thresholds)  # тут же проверяется, что модели на месте
    log.info("thresholds", extra={"fields": vars(thresholds)})

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    analyzer_pb2_grpc.add_AnalyzerServicer_to_server(AnalyzerService(analyzer, s3), server)

    # Стандартная gRPC-проверка здоровья — её умеют дёргать пробы Kubernetes.
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    server.add_insecure_port(addr)
    server.start()
    for name in ("", "analyzer.v1.Analyzer"):
        health_servicer.set(name, health_pb2.HealthCheckResponse.SERVING)
    log.info("listening", extra={"fields": {"addr": addr, "workers": workers, "version": VERSION}})

    def stop(signum, _frame):
        log.info("shutting down", extra={"fields": {"signal": signum}})
        for name in ("", "analyzer.v1.Analyzer"):
            health_servicer.set(name, health_pb2.HealthCheckResponse.NOT_SERVING)
        server.stop(grace=10)  # дать доделать текущие запросы

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
