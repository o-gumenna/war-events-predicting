"""
isw_forecast_features.py
────────────────────────────────────────────────────────────────────────────────
Server-side script for generating ISW-derived features for the next 24 hours.

Pipeline:
  1. Scrapes the latest ISW Russian Offensive Campaign Assessment report
  2. Compares the report date against a local cache; exits if unchanged
  3. Cleans and vectorizes report text using a pre-fitted TF-IDF vectorizer
  4. Extracts threat category scores and geographic coverage features
  5. Computes lag and rolling features consistent with the training pipeline
  6. Broadcasts the daily feature row across 24 hourly timestamps (T+1…T+24)
  7. Saves output to CSV and JSON; updates cache and history files

Recommended schedule: cron 0 * * * *

Output columns:
  datetime, isw_report_date, isw_geo_unique_locations,
  isw_<23 regions>,
  threat_ballistic_missiles_lag1/lag3/roll7,
  threat_cruise_missiles_lag1/roll7,
  threat_drones_roll7,
  threat_energy_targets_lag3/roll7,
  threat_guided_bombs_lag1/lag3/roll7,
  threat_launch_activity_lag1/lag3/roll7,
  threat_military_targets_roll7,
  threat_naval_carriers_lag1/lag3/roll7
────────────────────────────────────────────────────────────────────────────────
"""

import re
import json
import time
import logging
import os
import pickle
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup


# ── Configuration ─────────────────────────────────────────────────────────────

CATALOG_URL  = "https://understandingwar.org/backgrounder/russian-offensive-campaign-assessment"
BASE_URL     = "https://understandingwar.org"
OUTPUT_CSV   = Path("data/isw/isw_forecast_features.csv")
OUTPUT_JSON  = Path("data/isw/isw_forecast_features.json")
CACHE_FILE   = Path("data/isw/.isw_cache.json")
HISTORY_FILE = Path("data/isw/isw_history.json")
HOURS_AHEAD  = 24
REQUEST_DELAY = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("isw_forecast")

# ── Regular expressions ───────────────────────────────────────────────────────

RE_TITLE_DATE = re.compile(
    r"(january|february|march|april|may|june|july|august|september|"
    r"october|november|december)\s+(\d{1,2})",
    re.IGNORECASE,
)
RE_YEAR  = re.compile(r"\b(20\d{2})\b")
RE_TIME  = re.compile(r"(\d{1,2})(?::\d{2})?\s*(am|pm)\s*E[DS]?T", re.IGNORECASE)
RE_URL   = re.compile(r"http\S+")
RE_DIGIT = re.compile(r"\d+")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

# ── Geographic patterns compiled once at import time ─────────────────────────
# Each pattern covers common transliteration variants for the region name.

GEO_REGEX: dict[str, str] = {
    "Cherkasy":        r"\b(?:cherkasy|cherkassy)\b",
    "Chernihiv":       r"\b(?:chernihiv|chernigov)\b",
    "Chernivtsi":      r"\b(?:chernivtsi|chernovtsy)\b",
    "Dnipro":          r"\b(?:dnipro|dnipropetrovsk)\b",
    "Donetsk":         r"\b(?:donetsk)\b",
    "Ivano-Frankivsk": r"\b(?:ivano-frankivsk|ivano frankivsk)\b",
    "Kharkiv":         r"\b(?:kharkiv|kharkov)\b",
    "Kherson":         r"\b(?:kherson)\b",
    "Khmelnytskyi":    r"\b(?:khmelnytskyi|khmelnitsky)\b",
    "Kropyvnytskyi":   r"\b(?:kropyvnytskyi|kirovohrad)\b",
    "Kyiv":            r"\b(?:kyiv|kiev)\b",
    "Lutsk":           r"\b(?:lutsk|volyn)\b",
    "Lviv":            r"\b(?:lviv|lvov)\b",
    "Mykolaiv":        r"\b(?:mykolaiv|nikolaev)\b",
    "Odesa":           r"\b(?:odesa|odessa)\b",
    "Poltava":         r"\b(?:poltava)\b",
    "Rivne":           r"\b(?:rivne|rovno)\b",
    "Sumy":            r"\b(?:sumy)\b",
    "Ternopil":        r"\b(?:ternopil)\b",
    "Uzhhorod":        r"\b(?:uzhhorod|uzhgorod|zakarpattia)\b",
    "Vinnytsia":       r"\b(?:vinnytsia|vinnitsa)\b",
    "Zaporizhzhia":    r"\b(?:zaporizhzhia|zaporizhia|zaporozhye)\b",
    "Zhytomyr":        r"\b(?:zhytomyr|zhitomir)\b",
}

GEO_PATTERNS = {city: re.compile(pat, re.IGNORECASE) for city, pat in GEO_REGEX.items()}

# ── Stop words for text normalization ────────────────────────────────────────

STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "was", "are", "were", "be", "been",
    "has", "have", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "that", "this", "these", "those", "it", "its",
    "as", "not", "also", "their", "they", "them", "which", "who", "what",
    "when", "where", "how", "if", "than", "then", "so", "into", "out",
    "about", "over", "after", "before", "during", "along", "across",
    "through", "between", "against", "according", "said", "report",
    "forces", "russian", "ukraine", "ukrainian",
}

# ── Threat category to lag/roll variant mapping ───────────────────────────────
# Only the combinations retained after leakage removal in the training pipeline.

THREAT_LAG_MAP = {
    "ballistic_missiles": ["lag1", "lag3", "roll7"],
    "cruise_missiles":    ["lag1", "roll7"],
    "drones":             ["roll7"],
    "energy_targets":     ["lag3", "roll7"],
    "guided_bombs":       ["lag1", "lag3", "roll7"],
    "launch_activity":    ["lag1", "lag3", "roll7"],
    "military_targets":   ["roll7"],
    "naval_carriers":     ["lag1", "lag3", "roll7"],
}

# ══════════════════════════════════════════════════════════════════════════════
# Scraping utilities
# ══════════════════════════════════════════════════════════════════════════════

def _get_soup(url: str) -> BeautifulSoup | None:
    """Fetch a page and return a BeautifulSoup object, or None on failure."""
    try:
        resp = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0 (research bot)"},
        )
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as exc:
        log.error("HTTP error for %s: %s", url, exc)
        return None


def find_latest_report_url() -> str | None:
    """
    Locate the URL of the most recent ISW Russian Offensive Campaign Assessment
    by scanning anchor tags on the catalog page.
    Returns the full URL string or None if not found.
    """
    soup = _get_soup(CATALOG_URL)
    if soup is None:
        return None

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if "Russian Offensive Campaign Assessment" in text:
            href = a["href"].split("?")[0].rstrip("/")
            full = BASE_URL + href if href.startswith("/") else href
            return full

    log.warning("Report link not found on catalog page.")
    return None


def scrape_report(url: str) -> dict | None:
    """
    Download a report page and extract its publication date and body text.
    The date is parsed from the H1 heading (month + day); the year is located
    by scanning all text nodes on the page. Returns a dict with keys:
    date (ISO string), text (str), url (str).
    """
    soup = _get_soup(url)
    if soup is None:
        return None

    h1    = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""
    dm    = RE_TITLE_DATE.search(title)
    if not dm:
        log.error("Date not found in heading: %s", title)
        return None

    month = MONTHS[dm.group(1).lower()]
    day   = int(dm.group(2))

    year = None
    for el in soup.find_all(string=RE_YEAR):
        ym = RE_YEAR.search(el)
        if ym:
            year = int(ym.group(1))
            break

    if year is None:
        log.error("Year not found on page: %s", url)
        return None

    try:
        from datetime import date
        report_date = date(year, month, day).isoformat()
    except ValueError as exc:
        log.error("Invalid date %d/%d/%d: %s", day, month, year, exc)
        return None

    # Extract main article text from the content container
    content = (
        soup.find("div", class_=re.compile(r"field--name-body|field-name-body|field-items"))
        or soup.find("article")
    )
    if content:
        for tag in content(["script", "style", "nav", "footer"]):
            tag.decompose()
        text = content.get_text(separator=" ", strip=True)
    else:
        text = soup.get_text(separator=" ", strip=True)

    log.info("Scraped report dated %s (%d words)", report_date, len(text.split()))
    return {"date": report_date, "text": text, "url": url}


# ══════════════════════════════════════════════════════════════════════════════
# Text processing
# ══════════════════════════════════════════════════════════════════════════════

def clean_text(text: str) -> str:
    """
    Normalize report text to match the preprocessing applied during training:
    remove URLs and digit tokens, lowercase, filter stop words.
    """
    text   = RE_URL.sub(" ", text)
    text   = RE_DIGIT.sub(" ", text)
    text   = text.lower()
    tokens = [w for w in text.split() if w.isalpha() and w not in STOP_WORDS]
    return " ".join(tokens)


# Load artifacts saved during training

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
VECTORIZER_PATH = os.path.join(SCRIPT_DIR, "isw_tfidf_vectorizer.pkl")
KW_DICT_PATH = os.path.join(SCRIPT_DIR, "isw_kw_dict.pkl")

vectorizer = pickle.load(open(VECTORIZER_PATH, "rb"))
kw_dict = pickle.load(open(KW_DICT_PATH, "rb"))

vocab = vectorizer.vocabulary_


def score_tfidf(cleaned_text: str) -> dict[str, float]:
    """
    Transform cleaned text with the training-fitted TF-IDF vectorizer and
    aggregate scores per threat category by averaging the TF-IDF values of
    the category's keyword bigrams that are present in the vocabulary.
    Returns a dict keyed by threat_{category}.
    """
    arr    = vectorizer.transform([cleaned_text]).toarray()[0]
    scores = {}
    for category, keywords in kw_dict.items():
        col_indices = [vocab[kw] for kw in keywords if kw in vocab]
        scores[f"threat_{category}"] = (
            float(np.mean(arr[col_indices])) if col_indices else 0.0
        )
    return scores


def extract_geo_features(raw_text: str) -> dict[str, int]:
    """
    Produce binary presence flags for each of the 23 Ukrainian regions
    and a total count of unique locations mentioned in the report.
    """
    feats  = {}
    unique = 0
    for city, pat in GEO_PATTERNS.items():
        found = 1 if pat.search(raw_text) else 0
        feats[f"isw_{city}"] = found
        unique += found
    feats["isw_geo_unique_locations"] = unique
    return feats


def extract_publish_hour_utc(raw_text: str) -> int:
    """
    Extract the publication timestamp from the report header (ET timezone)
    and convert to a UTC hour integer. Returns 20 as a default when no
    timestamp is found, consistent with the typical ISW evening schedule.
    """
    m = RE_TIME.search(raw_text)
    if not m:
        return 20
    hour   = int(m.group(1))
    period = m.group(2).lower()
    if period == "pm" and hour != 12:
        hour += 12
    elif period == "am" and hour == 12:
        hour = 0
    return (hour + 5) % 24  # convert ET (UTC-5) to UTC


# ══════════════════════════════════════════════════════════════════════════════
# Cache and history persistence
# ══════════════════════════════════════════════════════════════════════════════

def load_cache() -> dict:
    """Load the run cache from disk. Returns an empty dict if unavailable."""
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text())
        except Exception:
            pass
    return {}


def save_cache(data: dict) -> None:
    """Persist run metadata (last report date, timestamp, URL) to disk."""
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(data, indent=2))


def load_history() -> list[dict]:
    """
    Load the rolling history of daily threat scores (up to 7 days).
    Used to compute lag and rolling mean features at inference time.
    """
    if HISTORY_FILE.exists():
        try:
            return json.loads(HISTORY_FILE.read_text())
        except Exception:
            pass
    return []


def save_history(history: list[dict]) -> None:
    """Persist the most recent 7 daily threat score records to disk."""
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_FILE.write_text(json.dumps(history[-7:], indent=2))


# ══════════════════════════════════════════════════════════════════════════════
# Lag and rolling feature computation
# ══════════════════════════════════════════════════════════════════════════════

def compute_lag_roll_features(
    today_threats: dict[str, float],
    history: list[dict],
) -> dict[str, float]:
    """
    Compute lag and rolling features to replicate the training pipeline.

    In training:
      lag1  = shift(1): the previous day's threat score
      lag3  = shift(3): the score from 3 days prior
      roll7 = rolling(7).mean(): 7-day mean including the current day

    At inference the latest scraped report is the current day (T).
    The same report acts as the lag-1 signal for the next 24-hour window,
    consistent with the backward-fill logic used during dataset construction.
      lag1  = today_threats
      lag3  = history[-2] (two days before today)
      roll7 = mean(history[-6:] + [today])

    Only the lag/roll combinations defined in THREAT_LAG_MAP are generated,
    matching the feature set retained after leakage removal.
    """
    result = {}

    def get_val(rec: dict, name: str) -> float:
        return float(rec.get(name, 0.0))

    for category, variants in THREAT_LAG_MAP.items():
        name = f"threat_{category}"

        val_today = today_threats.get(name, 0.0)
        val_lag1  = val_today
        val_lag3  = get_val(history[-2], name) if len(history) >= 2 else 0.0
        window    = [get_val(r, name) for r in history[-6:]] + [val_today]
        val_roll7 = sum(window) / len(window)

        if "lag1"  in variants: result[f"{name}_lag1"]  = round(val_lag1,  6)
        if "lag3"  in variants: result[f"{name}_lag3"]  = round(val_lag3,  6)
        if "roll7" in variants: result[f"{name}_roll7"] = round(val_roll7, 6)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Hourly row generation
# ══════════════════════════════════════════════════════════════════════════════

def build_hourly_rows(
    report_date_str:  str,
    publish_hour_utc: int,
    geo_feats:        dict,
    lag_roll_feats:   dict,
) -> list[dict]:
    """
    Broadcast the daily ISW feature vector across 24 hourly timestamps (T+1…T+24).
    Each row receives an identical copy of the features since reports are published
    once per day and cover the entire following 24-hour window.
    All datetimes are UTC, floored to the hour.
    """
    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    rows    = []
    for h in range(1, HOURS_AHEAD + 1):
        dt  = now_utc + timedelta(hours=h)
        row = {"datetime": dt.isoformat(), "isw_report_date": report_date_str}
        row.update(geo_feats)
        row.update(lag_roll_feats)
        rows.append(row)
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

def save_outputs(rows: list[dict]) -> None:
    """Write feature rows to CSV and JSON output files."""
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUTPUT_CSV, index=False)
    log.info("Saved CSV: %s (%d rows)", OUTPUT_CSV, len(rows))

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    log.info("Saved JSON: %s", OUTPUT_JSON)


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("=== ISW forecast features — start ===")

    report_url = find_latest_report_url()
    if not report_url:
        log.error("Could not locate report URL. Exiting.")
        return
    log.info("Report URL: %s", report_url)

    cache = load_cache()

    # old report
    if cache.get("report_url") == report_url and "geo_feats" in cache and "lag_roll" in cache:
        log.info("Report is unchanged. Rebuilding 24h grid from cache...")
        rows = build_hourly_rows(
            report_date_str=cache["last_report_date"],
            publish_hour_utc=cache.get("publish_hour", 20),
            geo_feats=cache["geo_feats"],
            lag_roll_feats=cache["lag_roll"],
        )
        save_outputs(rows)
        log.info("=== Done (Cached). Grid shifted to current time ===")
        return

    # new report published
    time.sleep(REQUEST_DELAY)
    report = scrape_report(report_url)
    if not report:
        log.error("Could not scrape report. Exiting.")
        return

    log.info("New report: %s. Processing...", report["date"])

    raw_text = report["text"]
    cleaned = clean_text(raw_text)
    publish_hour = extract_publish_hour_utc(raw_text)

    today_threats = score_tfidf(cleaned)
    log.info("Threat scores: %s", {k: f"{v:.5f}" for k, v in today_threats.items()})

    geo_feats = extract_geo_features(raw_text)

    history = load_history()
    lag_roll = compute_lag_roll_features(today_threats, history)

    rows = build_hourly_rows(
        report_date_str=report["date"],
        publish_hour_utc=publish_hour,
        geo_feats=geo_feats,
        lag_roll_feats=lag_roll,
    )

    save_outputs(rows)

    history.append({"date": report["date"], **today_threats})
    save_history(history)

    save_cache({
        "last_report_date": report["date"],
        "last_run_utc": datetime.now(timezone.utc).isoformat(),
        "report_url": report_url,
        "publish_hour": publish_hour,
        "geo_feats": geo_feats,
        "lag_roll": lag_roll
    })

    log.info("=== Done (New). Report %s → %d rows for next 24h ===", report["date"], len(rows))


if __name__ == "__main__":
    main()