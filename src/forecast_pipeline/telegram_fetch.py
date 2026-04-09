#!/usr/bin/env python3
"""Telegram fetching and feature generation (UTC timestamps).

Collect raw Telegram messages, aggregate hourly per city, and generate
lag features for forecasting.
"""

import os
import re
import logging
import asyncio
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv


try:
    from telethon.sync import TelegramClient
except Exception:
    TelegramClient = None

load_dotenv()

# Configuration

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

# City patterns (regex)

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


ALL_CITIES = [
    'Cherkasy', 'Chernihiv', 'Chernivtsi', 'Dnipro', 'Donetsk', 'Ivano-Frankivsk',
    'Kharkiv', 'Kherson', 'Khmelnytskyi', 'Kropyvnytskyi', 'Kyiv', 'Lutsk',
    'Lviv', 'Mykolaiv', 'Odesa', 'Poltava', 'Rivne', 'Sumy', 'Ternopil',
    'Uzhhorod', 'Vinnytsia', 'Zaporizhzhia', 'Zhytomyr'
]

# Threat patterns

THREAT_DICT = {
    'tg_shaheds': ['шахед', 'герань', 'мопед', 'бпла', 'безпілотник', 'дрон', 'камікадзе'],
    'tg_ballistic': ['балістик', 'балістичн', 'іскандер', 'с-300', 'с-400', 'оперативно-тактичн'],
    'tg_mig31': ['міг-31', 'кинджал', 'саваслейка', 'моздок', 'аеробалістич'],
    'tg_cruise': ['крилат', 'калібр', 'х-101', 'х-555', 'ту-95', 'ту-22', 'ракет'],
    'tg_all_clear': ['відбій'],
}

# Only feature threats (exclude all_clear from the "clean signal" step,
# but include it in lag features since model expects tg_all_clear_count_lag1h/lag3h)
FEATURE_THREATS = ['tg_shaheds', 'tg_ballistic', 'tg_mig31', 'tg_cruise']
LAG_THREATS = FEATURE_THREATS + ['tg_all_clear']  # all_clear included in lag output


# Text processing

def get_ukrainian_stopwords():
    """Return a set of Ukrainian stopwords."""
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
    """Simple text cleaning and stopword removal."""
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
    """Return list of matched cities or all cities for nationwide messages."""
    if not text_clean:
        return ALL_CITIES  # Nationwide message

    found = [
        city for city, pattern in CITY_PATTERNS_FULL.items()
        if re.search(pattern, text_clean, re.IGNORECASE)
    ]
    return found if found else ALL_CITIES


def count_threat(text, keywords):
    """Return 1 if any keyword appears in text, else 0."""
    if not isinstance(text, str) or text.strip() == '':
        return 0
    return int(any(kw in text for kw in keywords))


# Timestamps are UTC; store as naive datetimes (drop tzinfo).


# Telegram scraping

async def scrape_messages(lookback_hours: int, stopwords) -> list[dict]:
    """Scrape Telegram messages from the last N hours."""
    if not API_ID or not API_HASH:
        log.error("TG_API_ID or TG_API_HASH not configured!")
        return []

    log.info(f"Connecting to Telegram to scrape last {lookback_hours}h...")

    # use naive UTC timestamps
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

                # Minimal handling: Telethon повертає tz-aware datetime (UTC).
                # Зберігаємо tz-aware UTC — стандарт пайплайну.
                date_utc = msg_dt if msg_dt.tzinfo is not None else msg_dt.replace(tzinfo=timezone.utc)

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


# Stage 1: collect raw data

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

    # Round to hour — зберігаємо tz-aware UTC
    df_exploded['datetime'] = pd.to_datetime(df_exploded['datetime'], utc=True).dt.floor('h')

    # Лічильник повідомлень (unique msg_id per city per hour)
    df_exploded['tg_msg_count'] = 1

    # CRITICAL: SUM AGGREGATION (FROM NOTEBOOK, NOT BINARY MAX)
    agg_dict = {threat: 'sum' for threat in list(THREAT_DICT.keys())}
    agg_dict['tg_msg_count'] = 'sum'
    hourly = df_exploded.groupby(['datetime', 'city']).agg(agg_dict).reset_index()

    # Rename threat columns to add "_count" suffix (tg_msg_count already has correct name)
    rename_dict = {threat: f'{threat}_count' for threat in THREAT_DICT.keys()}
    hourly = hourly.rename(columns=rename_dict)

    return hourly


def collect_raw_data():
    """Scrape Telegram and update rolling 48h hourly history."""
    log.info("=" * 60)
    log.info("STAGE 1: COLLECTING RAW TELEGRAM DATA")
    log.info("=" * 60)

    # Use tz-aware UTC — стандарт пайплайну
    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

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
        # utc=True коректно обробляє і "+00:00" рядки, і naive
        df_hist['datetime'] = pd.to_datetime(df_hist['datetime'], utc=True).dt.floor('h')
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
    # utc=True коректно обробляє і "+00:00" рядки, і naive
    df_combined['datetime'] = pd.to_datetime(df_combined['datetime'], utc=True).dt.floor('h')
    df_combined = df_combined[df_combined['datetime'] >= cutoff]

    # Fill missing hours with zeros for all cities (tz-aware range)
    all_hours = pd.date_range(
        start=df_combined['datetime'].min(),
        end=now_utc,
        freq='h',
        tz='UTC'
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


# Stage 2: generate features

def generate_features():
    """Generate lag features for T+1..T+24.

    Lag rules:
    - lag1h: value at T (used for T+1)
    - lag3h: value at T-2..T (used for T+1..T+3)
    - lag6h: value at T-5..T (used for T+1..T+6)
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
    # utc=True коректно обробляє і "+00:00" рядки, і naive
    df_raw['datetime'] = pd.to_datetime(df_raw['datetime'], utc=True).dt.floor('h')

    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    log.info(f"Reference time (T=0): {now_utc}")
    log.info(f"Loaded {len(df_raw)} raw hourly records")

    # Generate future hours grid (T+1 to T+24) — tz-aware UTC
    future_hours = pd.date_range(
        start=now_utc + timedelta(hours=1),
        periods=FORECAST_HOURS,
        freq='h',
        tz='UTC'
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

            for threat in LAG_THREATS:  # включає tg_all_clear для lag1h і lag3h
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

                # lag6h: існує тільки для threat-типів (не для tg_all_clear)
                if threat in FEATURE_THREATS:
                    if hours_ahead <= 6:
                        lag6_dt = now_utc - timedelta(hours=(6 - hours_ahead))
                        lag6_val = city_hist[city_hist['datetime'] == lag6_dt][threat_col].values
                        row_data[f'{threat}_count_lag6h'] = int(lag6_val[0]) if len(lag6_val) > 0 else 0
                    else:
                        row_data[f'{threat}_count_lag6h'] = float('nan')

            # tg_msg_count lags (lag1h, lag3h, lag6h)
            msg_col = 'tg_msg_count'
            if msg_col in city_hist.columns:
                if hours_ahead == 1:
                    v = city_hist[city_hist['datetime'] == now_utc][msg_col].values
                    row_data['tg_msg_count_lag1h'] = int(v[0]) if len(v) > 0 else 0
                else:
                    row_data['tg_msg_count_lag1h'] = float('nan')

                if hours_ahead <= 3:
                    lag3_dt = now_utc - timedelta(hours=(3 - hours_ahead))
                    v = city_hist[city_hist['datetime'] == lag3_dt][msg_col].values
                    row_data['tg_msg_count_lag3h'] = int(v[0]) if len(v) > 0 else 0
                else:
                    row_data['tg_msg_count_lag3h'] = float('nan')

                if hours_ahead <= 6:
                    lag6_dt = now_utc - timedelta(hours=(6 - hours_ahead))
                    v = city_hist[city_hist['datetime'] == lag6_dt][msg_col].values
                    row_data['tg_msg_count_lag6h'] = int(v[0]) if len(v) > 0 else 0
                else:
                    row_data['tg_msg_count_lag6h'] = float('nan')
            else:
                # Якщо колонка відсутня в raw — NaN (HistGBM впорається)
                row_data['tg_msg_count_lag1h'] = float('nan')
                row_data['tg_msg_count_lag3h'] = float('nan')
                row_data['tg_msg_count_lag6h'] = float('nan')

            all_rows.append(row_data)

    df_output = pd.DataFrame(all_rows)
    df_output = df_output.sort_values(['region', 'datetime']).reset_index(drop=True)

    FEATURES_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_output.to_csv(FEATURES_FILE, index=False)

    log.info(f"Saved features: {FEATURES_FILE}")
    log.info(f"Shape: {df_output.shape}")
    log.info(f"Regions: {len(df_output['region'].unique())}")
    log.info(f"Hours: {df_output['datetime'].min()} → {df_output['datetime'].max()}")

    # log lag coverage for first few lag columns
    lag_cols = [col for col in df_output.columns if 'lag' in col]
    for col in lag_cols[:3]:
        non_nan = df_output[col].notna().sum()
        total = len(df_output)
        log.info(f"{col}: {non_nan}/{total} non-NaN values ({100*non_nan/total:.1f}%)")

    log.info("=" * 60)


# Main entry

def main():
    """Main entry. Use 'collect' or 'generate' as argument."""
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