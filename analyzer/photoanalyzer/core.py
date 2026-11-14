"""Сам анализ картинки. Здесь нет ни gRPC, ни S3 — только numpy и OpenCV,
поэтому этот код легко проверять тестами и скриптом calibrate.py."""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path

import cv2
import numpy as np

log = logging.getLogger(__name__)

MODELS_DIR = Path(__file__).resolve().parent.parent / "models"
YUNET_PATH = MODELS_DIR / "face_detection_yunet_2023mar.onnx"
LANDMARKER_PATH = MODELS_DIR / "face_landmarker.task"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


@dataclass
class Thresholds:
    """Пороги. Начальные значения — прикидка, подбираются на своих фото (calibrate.py)."""

    blur: float = field(default_factory=lambda: _env_float("BLUR_THRESHOLD", 50.0))
    dark: float = field(default_factory=lambda: _env_float("DARK_THRESHOLD", 40.0))
    bright: float = field(default_factory=lambda: _env_float("BRIGHT_THRESHOLD", 215.0))
    face_score: float = field(default_factory=lambda: _env_float("FACE_SCORE_THRESHOLD", 0.8))
    eye_blink: float = field(default_factory=lambda: _env_float("EYE_BLINK_THRESHOLD", 0.5))
    # Лица меньше этого (в пикселях после уменьшения картинки) на глаза не проверяем: слишком мелко.
    min_face_for_eyes: float = field(default_factory=lambda: _env_float("MIN_FACE_FOR_EYES", 60.0))


@dataclass
class Result:
    width: int
    height: int
    blur_score: float
    is_blurry: bool
    faces_count: int
    eyes_closed_count: int
    mean_brightness: float
    is_underexposed: bool
    is_overexposed: bool
    phash: int  # 64 бита, без знака


# ---------- загрузка ----------


def decode(data: bytes) -> np.ndarray:
    """Байты файла → картинка BGR. Поворот по EXIF OpenCV применяет сам."""
    arr = np.frombuffer(data, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("cannot decode image")
    return img


def resize_max(img: np.ndarray, max_side: int) -> np.ndarray:
    """Уменьшает так, чтобы длинная сторона была не больше max_side. Не увеличивает."""
    h, w = img.shape[:2]
    scale = max_side / max(h, w)
    if scale >= 1:
        return img
    return cv2.resize(img, (round(w * scale), round(h * scale)), interpolation=cv2.INTER_AREA)


# ---------- смазанность, яркость, хеш ----------


def blur_score(gray: np.ndarray, grid: int = 4) -> float:
    """Резкость: дисперсия лапласиана (насколько много резких перепадов яркости).

    Считаем по клеткам сетки grid×grid и берём самую резкую клетку.
    Так портрет с размытым фоном не считается смазанным: главное, чтобы
    хоть где-то (на объекте съёмки) была резкость.
    """
    h, w = gray.shape
    best = 0.0
    for i in range(grid):
        for j in range(grid):
            tile = gray[i * h // grid:(i + 1) * h // grid, j * w // grid:(j + 1) * w // grid]
            if tile.size == 0:
                continue
            best = max(best, float(cv2.Laplacian(tile, cv2.CV_64F).var()))
    return best


def mean_brightness(gray: np.ndarray) -> float:
    return float(gray.mean())


def phash(gray: np.ndarray) -> int:
    """Перцептивный хеш, 64 бита.

    Уменьшаем до 32×32, раскладываем на частоты (DCT), берём 8×8 самых
    низких частот (общая «форма» картинки) и сравниваем каждую с медианой.
    Похожие картинки (пересжатие, уменьшение, лёгкая цветокоррекция)
    дают хеши, отличающиеся на несколько бит.
    """
    small = cv2.resize(gray, (32, 32), interpolation=cv2.INTER_AREA).astype(np.float32)
    dct = cv2.dct(small)
    low = dct[:8, :8].flatten()
    median = np.median(low[1:])  # без постоянной составляющей
    bits = low > median
    value = 0
    for bit in bits:
        value = (value << 1) | int(bit)
    return value


def hamming(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


# ---------- лица и глаза ----------


class FaceAnalyzer:
    """Лица ищет YuNet (OpenCV), закрытые глаза — FaceLandmarker (mediapipe).

    Ни YuNet, ни mediapipe нельзя дёргать из нескольких потоков одновременно,
    поэтому у каждого потока сервера свой экземпляр моделей.
    """

    def __init__(self, thresholds: Thresholds):
        self.t = thresholds
        self._local = threading.local()
        if not YUNET_PATH.exists():
            raise FileNotFoundError(f"нет модели {YUNET_PATH}, запусти download_models.py")
        self.eyes_enabled = LANDMARKER_PATH.exists()
        if not self.eyes_enabled:
            log.warning("нет модели %s: закрытые глаза проверяться не будут", LANDMARKER_PATH)

    def _yunet(self):
        det = getattr(self._local, "yunet", None)
        if det is None:
            det = cv2.FaceDetectorYN.create(str(YUNET_PATH), "", (320, 320), self.t.face_score, 0.3, 5000)
            self._local.yunet = det
        return det

    def _landmarker(self):
        lm = getattr(self._local, "landmarker", None)
        if lm is None:
            from mediapipe.tasks import python as mp_python
            from mediapipe.tasks.python import vision

            opts = vision.FaceLandmarkerOptions(
                base_options=mp_python.BaseOptions(model_asset_path=str(LANDMARKER_PATH)),
                running_mode=vision.RunningMode.IMAGE,
                num_faces=1,
                output_face_blendshapes=True,
            )
            lm = vision.FaceLandmarker.create_from_options(opts)
            self._local.landmarker = lm
        return lm

    def detect_faces(self, bgr: np.ndarray) -> np.ndarray:
        """Возвращает массив N×15 (x, y, w, h, 5 точек лица, уверенность)."""
        h, w = bgr.shape[:2]
        det = self._yunet()
        det.setInputSize((w, h))
        _, faces = det.detect(bgr)
        return faces if faces is not None else np.zeros((0, 15), dtype=np.float32)

    def blink_score(self, bgr: np.ndarray, face: np.ndarray) -> float | None:
        """Насколько закрыты глаза у одного лица: 0 — открыты, 1 — закрыты.
        Берём меньшее из двух глаз, чтобы подмигивание не считалось.
        None — не удалось оценить."""
        import mediapipe as mp

        x, y, fw, fh = face[:4]
        # Вырезаем лицо с запасом: FaceLandmarker лучше работает, когда лицо крупное
        cx, cy, side = x + fw / 2, y + fh / 2, max(fw, fh) * 1.6
        H, W = bgr.shape[:2]
        x0, y0 = int(max(0, cx - side / 2)), int(max(0, cy - side / 2))
        x1, y1 = int(min(W, cx + side / 2)), int(min(H, cy + side / 2))
        crop = bgr[y0:y1, x0:x1]
        if crop.size == 0:
            return None
        rgb = np.ascontiguousarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
        res = self._landmarker().detect(mp.Image(image_format=mp.ImageFormat.SRGB, data=rgb))
        if not res.face_blendshapes:
            return None
        scores = {c.category_name: c.score for c in res.face_blendshapes[0]}
        left, right = scores.get("eyeBlinkLeft"), scores.get("eyeBlinkRight")
        if left is None or right is None:
            return None
        return float(min(left, right))

    def analyze(self, bgr: np.ndarray) -> tuple[int, int, list[float | None]]:
        """→ (число лиц, сколько с закрытыми глазами, оценки по каждому лицу)."""
        faces = self.detect_faces(bgr)
        closed, blinks = 0, []
        for face in faces:
            if not self.eyes_enabled or min(face[2], face[3]) < self.t.min_face_for_eyes:
                blinks.append(None)
                continue
            b = self.blink_score(bgr, face)
            blinks.append(b)
            if b is not None and b >= self.t.eye_blink:
                closed += 1
        return len(faces), closed, blinks


# ---------- всё вместе ----------


class Analyzer:
    def __init__(self, thresholds: Thresholds | None = None, faces: bool = True):
        self.t = thresholds or Thresholds()
        self.faces = FaceAnalyzer(self.t) if faces else None

    def analyze_bytes(self, data: bytes) -> Result:
        return self.analyze(decode(data))

    def analyze(self, bgr: np.ndarray) -> Result:
        h, w = bgr.shape[:2]
        # Все метрики считаем на уменьшенной копии: быстрее, и пороги не зависят
        # от того, снято фото на 12 или на 50 мегапикселей.
        small = resize_max(bgr, 1024)
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)

        blur = blur_score(gray)
        bright = mean_brightness(gray)

        faces_count, closed = 0, 0
        if self.faces is not None:
            # Для лиц берём побольше: в групповых фото лица мелкие.
            faces_count, closed, _ = self.faces.analyze(resize_max(bgr, 1600))

        return Result(
            width=w,
            height=h,
            blur_score=round(blur, 2),
            is_blurry=blur < self.t.blur,
            faces_count=faces_count,
            eyes_closed_count=closed,
            mean_brightness=round(bright, 2),
            is_underexposed=bright < self.t.dark,
            is_overexposed=bright > self.t.bright,
            phash=phash(gray),
        )