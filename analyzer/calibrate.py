"""Прогоняет анализ по папке с фото и показывает все числа — чтобы подобрать пороги.

    python calibrate.py "C:\\Users\\acer\\Pictures\\test"

Пишет таблицу в calibration.csv (открывается в Excel) и печатает пары похожих фото.
Как пользоваться: посмотри, у каких фото blur маленький, а они на самом деле резкие
(или наоборот), и сдвинь порог BLUR_THRESHOLD. Так же с яркостью, глазами и хешем.
"""

from __future__ import annotations

import argparse
import csv
import itertools
from pathlib import Path

import cv2

from photoanalyzer.core import FaceAnalyzer, Thresholds, blur_score, decode, hamming, mean_brightness, phash, resize_max

EXTS = {".jpg", ".jpeg", ".png", ".webp"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("folder")
    ap.add_argument("--out", default="calibration.csv")
    ap.add_argument("--max-distance", type=int, default=12, help="показывать пары с расстоянием хешей не больше этого")
    args = ap.parse_args()

    t = Thresholds()
    faces = FaceAnalyzer(t)
    files = sorted(p for p in Path(args.folder).iterdir() if p.suffix.lower() in EXTS)
    if not files:
        raise SystemExit("в папке нет jpg/png/webp")

    rows, hashes = [], {}
    for p in files:
        try:
            img = decode(p.read_bytes())
        except ValueError:
            print(f"не читается: {p.name}")
            continue
        small = resize_max(img, 1024)
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        blur, bright, h = blur_score(gray), mean_brightness(gray), phash(gray)
        n_faces, closed, blinks = faces.analyze(resize_max(img, 1600))
        hashes[p.name] = h
        rows.append({
            "file": p.name,
            "blur": round(blur, 1),
            "blurry": blur < t.blur,
            "brightness": round(bright, 1),
            "dark": bright < t.dark,
            "bright": bright > t.bright,
            "faces": n_faces,
            "eyes_closed": closed,
            "blink_scores": " ".join("—" if b is None else f"{b:.2f}" for b in blinks),
            "phash": f"{h:016x}",
        })

    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter=";")
        w.writeheader()
        w.writerows(rows)

    print(f"\n{'файл':40} {'blur':>8} {'ярк.':>6} {'лиц':>4} {'закр.':>5}  глаза")
    for r in rows:
        flags = "".join([" СМАЗ" if r["blurry"] else "", " ТЕМН" if r["dark"] else "", " СВЕТЛ" if r["bright"] else ""])
        print(f"{r['file'][:40]:40} {r['blur']:>8} {r['brightness']:>6} {r['faces']:>4} {r['eyes_closed']:>5}  "
              f"{r['blink_scores']}{flags}")

    print(f"\nПохожие пары (расстояние ≤ {args.max_distance}; сейчас дубликатом считается ≤ 6):")
    pairs = sorted(
        (hamming(hashes[a], hashes[b]), a, b) for a, b in itertools.combinations(hashes, 2)
    )
    shown = [x for x in pairs if x[0] <= args.max_distance]
    for d, a, b in shown:
        print(f"  {d:2}  {a}  —  {b}")
    if not shown:
        print("  нет")
    print(f"\nТаблица: {Path(args.out).resolve()}")
    print(f"Текущие пороги: {vars(t)}")


if __name__ == "__main__":
    main()
