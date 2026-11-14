"""Проверки без моделей лиц: смазанность, яркость, хеш."""

import cv2
import numpy as np
import pytest

from photoanalyzer.core import Analyzer, Thresholds, blur_score, decode, hamming, phash, resize_max


def scene(seed=0, size=(600, 800)):
    """Картинка с текстурой и фигурами — похожа на фото больше, чем шум."""
    rng = np.random.default_rng(seed)
    img = np.full((*size, 3), 120, np.uint8)
    for _ in range(40):
        color = tuple(int(c) for c in rng.integers(0, 255, 3))
        center = (int(rng.integers(0, size[1])), int(rng.integers(0, size[0])))
        cv2.circle(img, center, int(rng.integers(10, 80)), color, -1)
    for _ in range(30):
        p1 = (int(rng.integers(0, size[1])), int(rng.integers(0, size[0])))
        p2 = (int(rng.integers(0, size[1])), int(rng.integers(0, size[0])))
        cv2.line(img, p1, p2, (255, 255, 255), 2)
    return img


def gray(img):
    return cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)


def jpeg(img, quality=80):
    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, quality])
    assert ok
    return buf.tobytes()


def test_blur_lower_for_blurred():
    sharp = gray(scene())
    blurred = cv2.GaussianBlur(sharp, (0, 0), 5)
    assert blur_score(sharp) > 5 * blur_score(blurred)


def test_blur_tiles_ignore_blurred_background():
    """Резкий объект в одном углу, остальное размыто — не смазано."""
    img = cv2.GaussianBlur(gray(scene(1)), (0, 0), 6)
    sharp_part = gray(scene(2))[:150, :200]
    img[:150, :200] = sharp_part
    whole_blurred = cv2.GaussianBlur(gray(scene(1)), (0, 0), 6)
    assert blur_score(img) > 5 * blur_score(whole_blurred)


def test_phash_stable_under_resize_and_jpeg():
    img = scene(3)
    h1 = phash(gray(img))
    h2 = phash(gray(decode(jpeg(resize_max(img, 400), quality=60))))
    assert hamming(h1, h2) <= 6


def test_phash_differs_for_different_images():
    assert hamming(phash(gray(scene(4))), phash(gray(scene(5)))) > 15


def test_phash_is_64_bit():
    h = phash(gray(scene(6)))
    assert 0 <= h < 2**64


def test_exposure():
    a = Analyzer(Thresholds(), faces=False)
    dark = a.analyze((scene(7) * 0.15).astype(np.uint8))
    bright = a.analyze(np.clip(scene(7).astype(int) + 170, 0, 255).astype(np.uint8))
    normal = a.analyze(scene(7))
    assert dark.is_underexposed and not dark.is_overexposed
    assert bright.is_overexposed and not bright.is_underexposed
    assert not normal.is_underexposed and not normal.is_overexposed


def test_analyze_bytes_full_result():
    a = Analyzer(Thresholds(), faces=False)
    r = a.analyze_bytes(jpeg(scene(8, size=(3000, 4000))))
    assert (r.width, r.height) == (4000, 3000)
    assert not r.is_blurry
    assert r.faces_count == 0


def test_decode_garbage():
    with pytest.raises(ValueError):
        decode(b"not an image")
