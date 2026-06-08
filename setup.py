"""Download MovieLens 25M into ./seeds/ and generate a synthetic users.csv.

Run once before `make build`:
    python setup.py

Idempotent: skips download if zip already present, skips synthetic users
generation if file already exists.
"""

from __future__ import annotations

import csv
import logging
import random
import shutil
import sys
import zipfile
from datetime import date, timedelta
from pathlib import Path

import requests


MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-25m.zip"
# Anchor to the project dir (this file's location), not the cwd — generate_users()
# is also called from the movielens_data_refresh Airflow task, whose working dir is
# not the project root, so relative paths raised FileNotFoundError on seeds/users.csv.
_ROOT = Path(__file__).resolve().parent
DATA_DIR = _ROOT / "data"
SEEDS_DIR = _ROOT / "seeds"
ZIP_PATH = DATA_DIR / "ml-25m.zip"
EXTRACT_DIR = DATA_DIR / "ml-25m"

WANTED = ["ratings.csv", "movies.csv", "tags.csv"]

TIERS = (
    ("basic", 0.30),
    ("standard", 0.40),
    ("premium", 0.30),
)
REGIONS = (
    ("NA", 0.40),
    ("EU", 0.30),
    ("APAC", 0.20),
    ("LATAM", 0.10),
)
DEVICES = ("desktop", "mobile", "tablet", "tv")


def _weighted_choice(options: tuple[tuple[str, float], ...]) -> str:
    r = random.random()
    cum = 0.0
    for value, weight in options:
        cum += weight
        if r <= cum:
            return value
    return options[-1][0]


def _random_date(start: date, end: date) -> date:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def download_dataset() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    if ZIP_PATH.exists():
        logging.info("Zip already at %s — skipping download.", ZIP_PATH)
        return
    logging.info("Downloading MovieLens 25M from %s …", MOVIELENS_URL)
    with requests.get(MOVIELENS_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        downloaded = 0
        with ZIP_PATH.open("wb") as fh:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                fh.write(chunk)
                downloaded += len(chunk)
                if total and downloaded % (10 * 1024 * 1024) < len(chunk):
                    logging.info("  %.0f%%", 100 * downloaded / total)
    logging.info("Downloaded %d bytes.", downloaded)


def extract_and_copy() -> dict[str, int]:
    if not EXTRACT_DIR.exists():
        logging.info("Extracting %s …", ZIP_PATH)
        with zipfile.ZipFile(ZIP_PATH) as zf:
            zf.extractall(DATA_DIR)
        candidates = list(DATA_DIR.glob("ml-25m*"))
        if candidates and candidates[0] != EXTRACT_DIR:
            candidates[0].rename(EXTRACT_DIR)

    SEEDS_DIR.mkdir(exist_ok=True)
    counts: dict[str, int] = {}
    for name in WANTED:
        src = EXTRACT_DIR / name
        if not src.exists():
            raise FileNotFoundError(f"{src} missing after extraction")
        dst = SEEDS_DIR / name
        shutil.copyfile(src, dst)
        with dst.open() as fh:
            counts[name] = sum(1 for _ in fh) - 1
    return counts


def generate_users(n: int = 162_541, seed: int = 42) -> int:
    out = SEEDS_DIR / "users.csv"
    if out.exists():
        logging.info("%s already exists — skipping user generation.", out)
        with out.open() as fh:
            return sum(1 for _ in fh) - 1

    random.seed(seed)
    start = date(2015, 1, 1)
    end = date(2024, 12, 31)
    with out.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["user_id", "membership_tier", "region", "device_preference", "created_at"])
        for user_id in range(1, n + 1):
            writer.writerow([
                user_id,
                _weighted_choice(TIERS),
                _weighted_choice(REGIONS),
                random.choice(DEVICES),
                _random_date(start, end).isoformat(),
            ])
    return n


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    download_dataset()
    counts = extract_and_copy()
    users_n = generate_users()
    logging.info("Seed counts:")
    for name, n in counts.items():
        logging.info("  %s: %d", name, n)
    logging.info("  users.csv: %d", users_n)
    logging.info("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("setup.py failed")
        sys.exit(1)
