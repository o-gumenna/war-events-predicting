#!/usr/bin/env python3
"""
telegram_fetch.py
────────────────────────────────────────────────────────────────────────────────
Script for collecting raw Telegram data and generating features for model input.

LOGIC MATCHES TELEGRAM_ANALYSIS NOTEBOOK:
- Text cleaning with Ukrainian stopwords (exact match)
- Threat patterns from notebook (comprehensive dictionaries)
- City patterns with oblast variants (full coverage)
- SUM aggregation (counts, not binary)
- Lag logic: lag1 for T+1, lag3 for T+1..T+3, lag6 for T+1..T+6

TWO-STAGE PIPELINE:
  Stage 1 (collect_raw_data): Collects RAW hourly aggregates (48h history)
  Stage 2 (generate_features): Processes raw data to create lag features for T+1...T+24

Execution:
  - collect_raw_data() → runs every 5 minutes
  - generate_features() → runs hourly
────────────────────────────────────────────────────────────────────────────────
"""

import os
import re
import logging
import asyncio
# Use stdlib timezone (UTC) — avoid pytz dependency
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Telethon is optional for running unit tests that don't call the scraper.
# Import lazily / optionally so tests can run on machines without telethon installed.
try:
    from telethon.sync import TelegramClient
except Exception:
    TelegramClient = None

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")

CHANNEL_USERNAME = 'air_alert_ua'

RAW_FILE = Path("data/telegram/telegram_raw_48h.csv")
FEATURES_FILE = Path("data/telegram/telegram_features_24h.csv")

RAW_HISTORY_HOURS = 48
FORECAST_HOURS = 24
SCRAPE_LOOKBACK_HOURS = 12

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("telegram_fetch")

# ── City Patterns (EXACT FROM NOTEBOOK) ───────────────────────────────────────

CITY_PATTERNS_FULL = {
    'Cherkasy': r'\b(?:черкас|черкащин)\w*',
    'Chernihiv': r'\b(?:чернігів|чернігов|чернігівщин)\w*',
    'Chernivtsi': r'\b(?:чернівц|буковин|чернівеччин)\w*',
    'Dnipro': r'\b(?:дніпр|дніпропетровськ|дніпропетровщин)\w*',
    'Donetsk': r'\b(?:донецьк|донець|донеччин)\w*',
    'Ivano-Frankivsk': r'\b(?:франківськ|прикарпатт|коломи|івано-франківщин|прикарпатськ)\w*',
    'Kharkiv': r'\b(?:харків|харков|чугуїв|купянськ|харківщин)\w*',
    'Kherson': r'\b(?:херсон|берислав|херсонщин)\w*',
    'Khmelnytskyi': r'\b(?:хмельницьк|старокостянтинів|шепетівк|хмельниччин)\w*',
    'Kropyvnytskyi': r'\b(?:кропивницьк|кіровоград|олександрі|кіровоградщин)\w*',
    'Kyiv': r'\b(?:київ|києва|васильків|білоцерків|київщин|броварах|біла церква|бровари)\w*',
    'Luhansk': r'\b(?:луганськ|луганщин)\w*',
    'Lutsk': r'\b(?:луцьк|ковель|волин|волинщин|волинськ)\w*',
    'Lviv': r'\b(?:львів|стрий|дрогобич|львівщин)\w*',
    'Mykolaiv': r'\b(?:миколаїв|очаків|вознесенськ|миколаївщин)\w*',
    'Odesa': r'\b(?:одес|ізмаїл|чорноморськ|одещин)\w*',
    'Poltava': r'\b(?:полтав|кременчу|миргород|полтавщин)\w*',
    'Rivne': r'\b(?:рівн|дубн|рівненщин)\w*',
    'Sumy': r'\b(?:сум|шостк|конотоп|охтирк|сумщин)\w*',
    'Ternopil': r'\b(?:тернопіль|тернопільщин)\w*',
    'Uzhhorod': r'\b(?:ужгород|мукачев|закарпат|закарпатськ)\w*',
    'Vinnytsia': r'\b(?:вінниц|жмеринк|вінниччин)\w*',
    'Zaporizhzhia': r'\b(?:запоріж|мелітопол|запорізьк)\w*',
    'Zhytomyr': r'\b(?:житомир|бердичів|коростен|житомирщин)\w*',
}

ALL_CITIES = sorted(CITY_PATTERNS_FULL.keys())

# ── Threat Patterns (EXACT FROM NOTEBOOK) ─────────────────────────────────────

THREAT_DICT = {
    'tg_shaheds': ['шахед', 'герань', 'мопед', 'бпла', 'безпілотник', 'дрон', 'камікадзе'],
    'tg_ballistic': ['балістик', 'балістичн', 'іскандер', 'с-300', 'с-400', 'оперативно-тактичн'],
    'tg_mig31': ['міг-31', 'кинджал', 'саваслейка', 'моздок', 'аеробалістич'],
    'tg_cruise': ['крилат', 'калібр', 'х-101', 'х-555', 'ту-95', 'ту-22', 'ракет'],
    'tg_all_clear': ['відбій'],
}

# Only feature threats (exclude all_clear)
FEATURE_THREATS = ['tg_shaheds', 'tg_ballistic', 'tg_mig31', 'tg_cruise']


# ── Text Processing (EXACT FROM NOTEBOOK) ─────────────────────────────────────

def get_ukrainian_stopwords():
    """Fetch Ukrainian stopwords from GitHub (EXACT FROM NOTEBOOK)."""
    try:
        url = 'https://raw.githubusercontent.com/skopytr/ukrainian-stopwords/master/ukrainian_stopwords.txt'
        response = requests.get(url, timeout=10)
        stopwords_ua = set(response.text.split('\n'))

        # Custom stopwords (EXACT FROM NOTEBOOK)
        custom_stopwords = {'підписатись', 'джерело', 'надіслати', 'канал', 'реклама', 'відео', 'фото'}
        stopwords_ua.update(custom_stopwords)

        return stopwords_ua
    except Exception as e:
        log.warning(f"Could not fetch stopwords: {e}. Using minimal set.")
        return {'і', 'в', 'на', 'з', 'у', 'по', 'до', 'від', 'та', 'що', 'як'}


def clean_text(text, stopwords):
    """Clean text (EXACT FROM NOTEBOOK)."""
    if not isinstance(text, str) or text.strip() == '':
        return ''
    text = text.lower()
    text = re.sub(r'http\S+|www\S+|@\w+', '', text)
    # Keep Ukrainian and English letters, spaces, and hyphens
    text = re.sub(r'[^а-яіїєґa-z\s-]', ' ', text)
    words = text.split()
    # Filter out stopwords and single-character tokens
    words = [w for w in words if w not in stopwords and len(w) > 1]
    return ' '.join(words)


def extract_cities(text_clean):
    """Extract cities from cleaned text (EXACT FROM NOTEBOOK)."""
    if not text_clean:
        return ALL_CITIES  # Nationwide message

    found = [
        city for city, pattern in CITY_PATTERNS_FULL.items()
        if re.search(pattern, text_clean, re.IGNORECASE)
    ]
    return found if found else ALL_CITIES


def count_threat(text, keywords):
    """Return 1 if at least one keyword is found, otherwise 0 (EXACT FROM NOTEBOOK)."""
    if not isinstance(text, str) or text.strip() == '':
        return 0
    return int(any(kw in text for kw in keywords))


# Note: we intentionally avoid timezone conversions here. Telethon returns
# message.date in UTC; we will drop tzinfo for storage and treat datetimes
# across the pipeline as naive UTC timestamps.


# ── Telegram Scraping ─────────────────────────────────────────────────────────

async def scrape_messages(lookback_hours: int, stopwords) -> list[dict]:
    """Scrape Telegram messages from the last N hours.

    This routine normalizes message timestamps to naive UTC datetimes using
    `normalize_to_utc_naive` so downstream aggregation stays consistent.
    """
    if not API_ID or not API_HASH:
        log.error("TG_API_ID or TG_API_HASH not configured!")
        return []

    log.info(f"Connecting to Telegram to scrape last {lookback_hours}h...")

    # Use naive UTC timestamps across the pipeline to avoid tz mismatches
    now_utc = datetime.utcnow()
    start_time_limit = now_utc - timedelta(hours=lookback_hours)
    limit_dt = start_time_limit

    messages_data = []

    script_dir = os.path.dirname(os.path.abspath(__file__))
    session_path = os.path.join(script_dir, 'alarm_session')

    try:
        async with TelegramClient(session_path, API_ID, API_HASH) as client:
            # iterate messages starting from now (Telethon returns aware datetimes often)
            async for message in client.iter_messages(CHANNEL_USERNAME, offset_date=now_utc):
                msg_dt = getattr(message, 'date', None)
                if msg_dt is None:
                    continue

                # Minimal handling: drop tzinfo if present and treat as UTC
                date_utc = msg_dt.replace(tzinfo=None)

                if date_utc < limit_dt:
                    break

                if not getattr(message, 'text', None):
                    continue

                text_clean = clean_text(message.text, stopwords)
                if not text_clean:
                    continue

                messages_data.append({
                    'datetime': date_utc,
                    'msg_id': message.id,
                    'text_clean': text_clean,
                })

        log.info(f"Scraped {len(messages_data)} messages")
        return messages_data

    except Exception as e:
        log.error(f"Telegram scraping error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 1: Collect and Save RAW Data (runs every 5 min)
# ══════════════════════════════════════════════════════════════════════════════

def process_messages_to_hourly(messages: list[dict]) -> pd.DataFrame:
    """
    Process raw messages into hourly aggregates per city.
    Uses SUM aggregation (EXACT FROM NOTEBOOK).
    """
    if not messages:
        return pd.DataFrame()

    df = pd.DataFrame(messages)

    # Extract cities from each message
    df['cities'] = df['text_clean'].apply(extract_cities)
    df_exploded = df.explode('cities').rename(columns={'cities': 'city'})

    # Apply threat detection for each type
    for threat_name, keywords in THREAT_DICT.items():
        df_exploded[threat_name] = df_exploded['text_clean'].apply(
            lambda t: count_threat(t, keywords)
        )

    # Handle "all clear" - it negates other threats
    clear_mask = df_exploded['tg_all_clear'] == 1
    df_exploded.loc[clear_mask, FEATURE_THREATS] = 0

    # Round to hour
    df_exploded['datetime'] = pd.to_datetime(df_exploded['datetime']).dt.floor('h')

    # CRITICAL: SUM AGGREGATION (FROM NOTEBOOK, NOT BINARY MAX)
    agg_dict = {threat: 'sum' for threat in list(THREAT_DICT.keys())}
    hourly = df_exploded.groupby(['datetime', 'city']).agg(agg_dict).reset_index()

    # Rename columns to add "_count" suffix
    rename_dict = {threat: f'{threat}_count' for threat in THREAT_DICT.keys()}
    hourly = hourly.rename(columns=rename_dict)

    return hourly


def collect_raw_data():
    """
    STAGE 1: Scrape Telegram, aggregate to hourly, update rolling 48h history.
    This runs frequently (e.g., every 5 minutes).
    """
    log.info("=" * 60)
    log.info("STAGE 1: COLLECTING RAW TELEGRAM DATA")
    log.info("=" * 60)

    # Use naive UTC across pipeline to avoid tz mismatch with Telethon datetimes
    now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    # Get stopwords
    stopwords = get_ukrainian_stopwords()

    # Scrape recent messages
    messages = asyncio.run(scrape_messages(SCRAPE_LOOKBACK_HOURS, stopwords))

    if not messages:
        log.warning("No messages scraped. Skipping update.")
        return

    # Process to hourly aggregates
    new_data = process_messages_to_hourly(messages)

    if new_data.empty:
        log.warning("No hourly data generated. Skipping update.")
        return

    # Load existing raw history
    if RAW_FILE.exists():
        df_hist = pd.read_csv(RAW_FILE)
        # Parse datetimes and ensure hour alignment as naive UTC
        df_hist['datetime'] = pd.to_datetime(df_hist['datetime']).dt.floor('h')
    else:
        df_hist = pd.DataFrame()

    # Merge new data with history
    if not df_hist.empty:
        df_combined = pd.concat([df_hist, new_data], ignore_index=True)
    else:
        df_combined = new_data.copy()

    # Remove duplicates (keep latest)
    df_combined = df_combined.drop_duplicates(subset=['datetime', 'city'], keep='last')

    # Keep only last 48 hours
    cutoff = now_utc - timedelta(hours=RAW_HISTORY_HOURS)
    # Ensure datetime column is parsed and floored to hour (naive UTC)
    df_combined['datetime'] = pd.to_datetime(df_combined['datetime']).dt.floor('h')
    df_combined = df_combined[df_combined['datetime'] >= cutoff]

    # Fill missing hours with zeros for all cities
    all_hours = pd.date_range(
        start=df_combined['datetime'].min(),
        end=now_utc,
        freq='h'
    )

    grid = pd.MultiIndex.from_product(
        [all_hours, ALL_CITIES],
        names=['datetime', 'city']
    )

    df_full = pd.DataFrame(index=grid).reset_index()
    df_full = df_full.merge(df_combined, on=['datetime', 'city'], how='left')

    # Fill NaN with 0 for threat counts
    count_cols = [f'{threat}_count' for threat in THREAT_DICT.keys()]
    for col in count_cols:
        if col in df_full.columns:
            df_full[col] = df_full[col].fillna(0).astype(int)

    df_full = df_full.sort_values(['city', 'datetime']).reset_index(drop=True)

    # Save raw data
    RAW_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_full.to_csv(RAW_FILE, index=False)

    log.info(f"Saved raw data: {RAW_FILE}")
    log.info(f"Total rows: {len(df_full)}")
    log.info(f"Time range: {df_full['datetime'].min()} → {df_full['datetime'].max()}")
    log.info("=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 2: Generate Features with Lags (runs hourly)
# ══════════════════════════════════════════════════════════════════════════════

def generate_features():
    """
    STAGE 2: Process raw 48h data to generate lag features for next 24h.

    LAG LOGIC (UNCHANGED FROM ORIGINAL):
    - For hour T+N (N hours into the future):
      - lag1h = value from T-1 (exists only for T+1)
      - lag3h = value from T-3 (exists for T+1...T+3)
      - lag6h = value from T-6 (exists for T+1...T+6)
      - If lag doesn't exist for that future hour → NaN
    """
    log.info("=" * 60)
    log.info("STAGE 2: GENERATING FEATURES FOR MODEL INPUT")
    log.info("=" * 60)

    if not RAW_FILE.exists():
        log.error(f"Raw data file not found: {RAW_FILE}")
        log.error("Run collect_raw_data() first!")
        return

    # Load raw historical data
    df_raw = pd.read_csv(RAW_FILE)
    # Parse datetimes as naive UTC and floor to hours for consistent comparisons
    df_raw['datetime'] = pd.to_datetime(df_raw['datetime']).dt.floor('h')

    now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    log.info(f"Reference time (T=0): {now_utc}")
    log.info(f"Loaded {len(df_raw)} raw hourly records")

    # Generate future hours grid (T+1 to T+24)
    future_hours = pd.date_range(
        start=now_utc + timedelta(hours=1),
        periods=FORECAST_HOURS,
        freq='h'
    )

    # For each future hour, calculate which lags should exist
    all_rows = []

    for city in ALL_CITIES:
        city_hist = df_raw[df_raw['city'] == city].copy()

        if city_hist.empty:
            log.warning(f"No historical data for {city}")
            continue

        city_hist = city_hist.sort_values('datetime').reset_index(drop=True)

        for idx, future_dt in enumerate(future_hours, start=1):
            hours_ahead = idx

            row_data = {
                'datetime': future_dt,
                'region': city,
            }

            for threat in FEATURE_THREATS:
                threat_col = f'{threat}_count'

                # lag1h: exists only for T+1
                if hours_ahead == 1:
                    lag1_dt = now_utc
                    lag1_val = city_hist[city_hist['datetime'] == lag1_dt][threat_col].values
                    row_data[f'{threat}_count_lag1h'] = int(lag1_val[0]) if len(lag1_val) > 0 else 0
                else:
                    row_data[f'{threat}_count_lag1h'] = float('nan')

                # lag3h: exists for T+1 to T+3
                if hours_ahead <= 3:
                    lag3_dt = now_utc - timedelta(hours=(3 - hours_ahead))
                    lag3_val = city_hist[city_hist['datetime'] == lag3_dt][threat_col].values
                    row_data[f'{threat}_count_lag3h'] = int(lag3_val[0]) if len(lag3_val) > 0 else 0
                else:
                    row_data[f'{threat}_count_lag3h'] = float('nan')

                # lag6h: exists for T+1 to T+6
                if hours_ahead <= 6:
                    lag6_dt = now_utc - timedelta(hours=(6 - hours_ahead))
                    lag6_val = city_hist[city_hist['datetime'] == lag6_dt][threat_col].values
                    row_data[f'{threat}_count_lag6h'] = int(lag6_val[0]) if len(lag6_val) > 0 else 0
                else:
                    row_data[f'{threat}_count_lag6h'] = float('nan')

            all_rows.append(row_data)

    df_output = pd.DataFrame(all_rows)
    df_output = df_output.sort_values(['region', 'datetime']).reset_index(drop=True)

    FEATURES_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_output.to_csv(FEATURES_FILE, index=False)

    log.info(f"Saved features: {FEATURES_FILE}")
    log.info(f"Shape: {df_output.shape}")
    log.info(f"Regions: {len(df_output['region'].unique())}")
    log.info(f"Hours: {df_output['datetime'].min()} → {df_output['datetime'].max()}")

    # Show lag coverage statistics
    lag_cols = [col for col in df_output.columns if 'lag' in col]
    for col in lag_cols[:3]:  # Show first 3 as example
        non_nan = df_output[col].notna().sum()
        total = len(df_output)
        log.info(f"{col}: {non_nan}/{total} non-NaN values ({100*non_nan/total:.1f}%)")

    log.info("=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# Main Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    """
    Main execution function.

    For cron setup:
    - Run collect_raw_data() every 5 minutes:   */5 * * * *
    - Run generate_features() every hour:       0 * * * *

    Or run both sequentially for testing.
    """
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "collect":
            collect_raw_data()
        elif sys.argv[1] == "generate":
            generate_features()
        else:
            log.error(f"Unknown command: {sys.argv[1]}")
            log.info("Usage: python telegram_fetch.py [collect|generate]")
    else:
        # Run both stages sequentially (for testing)
        log.info("Running both stages sequentially...")
        collect_raw_data()
        generate_features()


if __name__ == "__main__":
    main()