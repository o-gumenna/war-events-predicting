"""
collect_isw_history.py

One-time helper script for backfilling isw_history.json for the last N days.

Usage:
    python src/data_collection/collect_isw_history.py --days 7

What it does:
  1. Finds ISW report links on the catalog page
  2. Filters reports for the requested day range
  3. Parses each report into threat scores and geo features
  4. Saves the result to data/isw/isw_history.json in chronological order
  5. Updates the cache with the latest processed report

After that, run isw_fetch.py to generate the forecast features.
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

# Add src/forecast_pipeline to sys.path so this script can reuse the main helpers.
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent.parent
PIPELINE_DIR = REPO_ROOT / "src" / "forecast_pipeline"
sys.path.insert(0, str(PIPELINE_DIR))

# Reuse the existing parsing and feature helpers from isw_fetch.
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


# Find all report links from the catalog page.

def find_all_report_urls(days_back: int) -> list[tuple[str, str]]:
    """
    Return a list of (url, date_str) tuples for reports from the last days_back days.
    date_str is stored in ISO format (YYYY-MM-DD).
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

        # Extract month and day from the link text.
        dm = RE_TITLE_DATE.search(text)
        if not dm:
            continue

        # Resolve the year from the current calendar context.
        href = a["href"].split("?")[0].rstrip("/")
        full_url = BASE_URL + href if href.startswith("/") else href

        month = MONTHS[dm.group(1).lower()]
        day   = int(dm.group(2))

        # If the parsed month is ahead of the current month,
        # the report most likely belongs to the previous year.
        now = datetime.now(timezone.utc)
        year = now.year if month <= now.month else now.year - 1

        try:
            report_date = datetime(year, month, day, tzinfo=timezone.utc).date()
        except ValueError:
            continue

        if report_date < cutoff:
            continue

        results.append((full_url, report_date.isoformat()))

    # Keep the result in chronological order.
    results.sort(key=lambda x: x[1])
    log.info("Found %d reports in the last %d days.", len(results), days_back)
    return results


def collect_history(days_back: int, delay: float = REQUEST_DELAY) -> None:
    """Collect and save ISW history for the requested day range."""
    report_links = find_all_report_urls(days_back)

    if not report_links:
        log.warning("No reports found. Check catalog URL or increase --days.")
        return

    # Load existing history so repeated runs do not duplicate entries.
    existing_history = load_history()
    existing_dates = {entry["date"] for entry in existing_history}
    log.info("Existing history entries: %d", len(existing_history))

    new_entries = []
    last_report = None  # Keep the latest report for the cache update.

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

        # Save the latest processed report for cache refresh.
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

    # Merge old and new entries, then save them in chronological order.
    all_entries = existing_history + new_entries
    # Deduplicate by date and keep the latest copy.
    seen = {}
    for e in all_entries:
        seen[e["date"]] = e
    merged = sorted(seen.values(), key=lambda x: x["date"])

    save_history(merged)
    log.info("Saved history: %d entries total (added %d new).", len(merged), len(new_entries))

    # Refresh the cache if a newer report was processed.
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

    # Switch to the repository root so relative data paths resolve correctly.
    import os
    os.chdir(REPO_ROOT)
    log.info("Working directory: %s", REPO_ROOT)

    collect_history(days_back=args.days, delay=args.delay)
