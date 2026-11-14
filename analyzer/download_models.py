"""Скачивает две готовые модели в папку models/. Запустить один раз:  python download_models.py
Модели маленькие (≈4 МБ вместе), их можно закоммитить, чтобы потом не качать."""

import urllib.request
from pathlib import Path

MODELS = {
    # Детектор лиц YuNet из OpenCV Zoo, ~230 КБ
    "face_detection_yunet_2023mar.onnx":
        "https://github.com/opencv/opencv_zoo/raw/main/models/face_detection_yunet/face_detection_yunet_2023mar.onnx",
    # Точки лица и «насколько закрыт глаз» от Google MediaPipe, ~3,6 МБ
    "face_landmarker.task":
        "https://storage.googleapis.com/mediapipe-models/face_landmarker/face_landmarker/float16/1/face_landmarker.task",
}

out = Path(__file__).resolve().parent / "models"
out.mkdir(exist_ok=True)

for name, url in MODELS.items():
    path = out / name
    if path.exists() and path.stat().st_size > 0:
        print(f"уже есть: {name}")
        continue
    print(f"качаю {name} ...")
    urllib.request.urlretrieve(url, path)
    print(f"  готово, {path.stat().st_size / 1024:.0f} КБ")
