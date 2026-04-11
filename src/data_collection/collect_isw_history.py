"""
collect_isw_history.py
────────────────────────────────────────────────────────────────────────────────
Одноразовий скрипт для заповнення isw_history.json за останні N днів.

Запуск:
    python src/data_collection/collect_isw_history.py --days 7

Що робить:
  1. Знаходить усі посилання на звіти ISW на сторінці каталогу
  2. Фільтрує звіти за останні --days днів
  3. Парсить кожен звіт (text → TF-IDF → threat scores + geo_feats)
  4. Зберігає у data/isw/isw_history.json (хронологічний порядок)
  5. Оновлює .isw_cache.json останнім звітом

Після цього запустіть isw_fetch.py — він підхопить готову history.
────────────────────────────────────────────────────────────────────────────────
"""

import re
import sys
import json
import time
import pickle
import logging
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
import numpy as np
from bs4 import BeautifulSoup

# ── Додаємо src/forecast_pipeline у sys.path щоб імпортувати функції ─────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent.parent
PIPELINE_DIR = REPO_ROOT / "src" / "forecast_pipeline"
sys.path.insert(0, str(PIPELINE_DIR))

# Імпортуємо всі потрібні функції з isw_fetch
from isw_fetch import (
    CATALOG_URL, BASE_URL,
    HISTORY_FILE, CACHE_FILE,
    RE_TITLE_DATE, RE_YEAR, MONTHS,
    _get_soup, scrape_report, clean_text,
    score_tfidf, extract_geo_features, extract_publish_hour_utc,
    load_history, save_history, save_cache,
    compute_lag_roll_features,
    REQUEST_DELAY,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("isw_history_collector")


# ── Знаходимо всі посилання на звіти з каталогу ──────────────────────────────

def find_all_report_urls(days_back: int) -> list[tuple[str, str]]:
    """
    Повертає список (url, date_str) для звітів за останні days_back днів.
    date_str — ISO формат (YYYY-MM-DD), витягнутий з тексту посилання.
    """
    log.info("Scanning catalog page for report links...")
    soup = _get_soup(CATALOG_URL)
    if soup is None:
        log.error("Cannot load catalog page.")
        return []

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days_back)
    results = []

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if "Russian Offensive Campaign Assessment" not in text:
            continue

        # Витягуємо місяць і день з тексту посилання
        dm = RE_TITLE_DATE.search(text)
        if not dm:
            continue

        # Рік — шукаємо у href або беремо поточний/попередній
        href = a["href"].split("?")[0].rstrip("/")
        full_url = BASE_URL + href if href.startswith("/") else href

        month = MONTHS[dm.group(1).lower()]
        day   = int(dm.group(2))

        # Визначаємо рік: якщо місяць > поточного місяця — минулий рік
        now = datetime.now(timezone.utc)
        year = now.year if month <= now.month else now.year - 1

        try:
            report_date = datetime(year, month, day, tzinfo=timezone.utc).date()
        except ValueError:
            continue

        if report_date < cutoff:
            continue  # Старіший за потрібний діапазон

        results.append((full_url, report_date.isoformat()))

    # Хронологічний порядок (старіший — перший)
    results.sort(key=lambda x: x[1])
    log.info("Found %d reports in the last %d days.", len(results), days_back)
    return results


def collect_history(days_back: int, delay: float = REQUEST_DELAY) -> None:
    """Основна функція: збирає history за days_back днів і зберігає."""
    report_links = find_all_report_urls(days_back)

    if not report_links:
        log.warning("No reports found. Check catalog URL or increase --days.")
        return

    # Завантажуємо вже наявну history щоб не дублювати
    existing_history = load_history()
    existing_dates = {entry["date"] for entry in existing_history}
    log.info("Existing history entries: %d", len(existing_history))

    new_entries = []
    last_report = None  # Збережемо останній для cache

    for url, date_str in report_links:
        if date_str in existing_dates:
            log.info("  [SKIP] %s — already in history", date_str)
            continue

        log.info("  [FETCH] %s — %s", date_str, url)
        time.sleep(delay)

        report = scrape_report(url)
        if report is None:
            log.warning("  [FAIL] Could not scrape %s", url)
            continue

        raw_text = report["text"]
        cleaned  = clean_text(raw_text)

        today_threats = score_tfidf(cleaned)
        geo_feats     = extract_geo_features(raw_text)
        publish_hour  = extract_publish_hour_utc(raw_text)

        entry = {"date": date_str, **today_threats}
        new_entries.append(entry)

        # Запам'ятовуємо для cache
        last_report = {
            "date": date_str,
            "url": url,
            "today_threats": today_threats,
            "geo_feats": geo_feats,
            "publish_hour": publish_hour,
        }

        log.info("  [OK]   %s — threats: %s",
                 date_str,
                 {k.replace("threat_", ""): f"{v:.4f}" for k, v in today_threats.items()})

    if not new_entries:
        log.info("Nothing new to add. History is up to date.")
        return

    # Об'єднуємо і зберігаємо (без дублів, хронологічний порядок)
    all_entries = existing_history + new_entries
    # Видаляємо дублікати за датою (залишаємо останній)
    seen = {}
    for e in all_entries:
        seen[e["date"]] = e
    merged = sorted(seen.values(), key=lambda x: x["date"])

    save_history(merged)
    log.info("Saved history: %d entries total (added %d new).", len(merged), len(new_entries))

    # Оновлюємо cache якщо є новіший звіт
    if last_report is not None:
        history_for_lag = [e for e in merged if e["date"] < last_report["date"]]
        lag_roll = compute_lag_roll_features(last_report["today_threats"], history_for_lag)

        save_cache({
            "last_report_date": last_report["date"],
            "last_run_utc": datetime.now(timezone.utc).isoformat(),
            "report_url": last_report["url"],
            "publish_hour": last_report["publish_hour"],
            "geo_feats": last_report["geo_feats"],
            "today_threats": last_report["today_threats"],
            "lag_roll": lag_roll,
        })
        log.info("Updated cache with latest report: %s", last_report["date"])

    log.info("=== Done! Run isw_fetch.py to generate forecast features. ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect ISW report history for the last N days."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="How many days back to collect (default: 7, max useful: ~30)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY,
        help=f"Delay between HTTP requests in seconds (default: {REQUEST_DELAY})",
    )
    args = parser.parse_args()

    if args.days < 1 or args.days > 30:
        print("ERROR: --days must be between 1 and 30")
        sys.exit(1)

    # Переходимо в корінь репозиторію — щоб data/ paths були правильні
    import os
    os.chdir(REPO_ROOT)
    log.info("Working directory: %s", REPO_ROOT)

    collect_history(days_back=args.days, delay=args.delay)
