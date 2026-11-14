"""Ручная проверка запущенного сервера: просит проанализировать файл из хранилища.

    python try_client.py masha/39390da4-6045-417b-95a1-57893f7b9386

Аргумент — object_key, то есть «пользователь/id фото» (виден в ответе api-service и в Kafka)."""

import sys
from pathlib import Path

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parent / "gen"))
import analyzer_pb2  # noqa: E402
import analyzer_pb2_grpc  # noqa: E402

key = sys.argv[1] if len(sys.argv) > 1 else sys.exit("укажи object_key, например masha/<id>")
with grpc.insecure_channel("localhost:50051") as ch:
    stub = analyzer_pb2_grpc.AnalyzerStub(ch)
    try:
        r = stub.Analyze(analyzer_pb2.AnalyzeRequest(photo_id=key.split("/")[-1], bucket="photos", object_key=key), timeout=30)
    except grpc.RpcError as e:
        sys.exit(f"ошибка: {e.code().name}: {e.details()}")
print(r)
