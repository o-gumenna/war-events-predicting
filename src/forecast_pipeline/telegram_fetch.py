"""
telegram_fetch_binary.py
───────────────────────────────────────────────────────────────────────────────
FULL VERSION:
- 48h rolling raw data
- timezone-aware (UTC safe)
- binary features (0/1 instead of counts)
- dynamic lag logic (T+1 … T+24)
- edge-case safe
───────────────────────────────────────────────────────────────────────────────
"""

import os
import re
import logging
import asyncio
import pytz
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from telethon.sync import TelegramClient


load_dotenv(find_dotenv())

# ── CONFIG ────────────────────────────────────────────────────────────────────

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")

CHANNEL_USERNAME = 'air_alert_ua'

RAW_FILE = Path("data/telegram/telegram_raw_48h.csv")
FEATURES_FILE = Path("data/telegram/telegram_features_24h.csv")

RAW_HISTORY_HOURS = 48
FORECAST_HOURS = 24
SCRAPE_LOOKBACK_HOURS = 12

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tg_pipeline")

# ── DICTS ─────────────────────────────────────────────────────────────────────

CITY_PATTERNS = {
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


ALL_CITIES = sorted(CITY_PATTERNS.keys())

THREAT_PATTERNS = {
    'shaheds': r'\b(?:шахед|дрон)\w*',
    'ballistic': r'\b(?:баліст)\w*',
    'mig31': r'\b(?:міг-31|кинджал)\w*',
    'cruise': r'\b(?:крилат|ракет)\w*',
    'all_clear': r'\b(?:відбій)\w*',
}

FEATURE_THREATS = ['shaheds', 'ballistic', 'mig31', 'cruise']


def convert_to_utc(dt_naive):
    kyiv_tz = pytz.timezone('Europe/Kyiv')
    try:
        dt_localized = kyiv_tz.localize(dt_naive, is_dst=None)
    except (pytz.exceptions.AmbiguousTimeError, pytz.exceptions.NonExistentTimeError):
        dt_localized = kyiv_tz.localize(dt_naive, is_dst=False)
    return dt_localized.astimezone(pytz.UTC).replace(tzinfo=None)

# ── TEXT ──────────────────────────────────────────────────────────────────────

def clean_text(text):
    if not isinstance(text, str) or not text.strip():
        return ''
    text = text.lower()
    text = re.sub(r'http\S+|www\S+|@\w+', '', text)
    text = re.sub(r'[^а-яіїєґa-z\s]', ' ', text)
    return text

def extract_cities(text):
    if not text:
        return ALL_CITIES
    found = [c for c, p in CITY_PATTERNS.items() if re.search(p, text)]
    return found if found else ALL_CITIES

# ── SCRAPE ────────────────────────────────────────────────────────────────────
                
async def scrape():
    now_utc = datetime.now(timezone.utc)
    limit = now_utc - timedelta(hours=SCRAPE_LOOKBACK_HOURS)

    messages = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    session_path = os.path.join(script_dir, 'alarm_session')

    async with TelegramClient(session_path, API_ID, API_HASH) as client:
        async for msg in client.iter_messages(CHANNEL_USERNAME):
            if msg.date < limit:
                break
            if not msg.text:
                continue

            date_utc = convert_to_utc(msg.date.replace(tzinfo=None))
            text_clean = clean_text(msg.text)

            if not text_clean:
                continue

            messages.append({
                'datetime': date_utc,
                'text': text_clean
            })

    return messages

# ── STAGE 1 ───────────────────────────────────────────────────────────────────

def collect_raw_data():
    log.info("Collecting RAW data...")

    messages = asyncio.run(scrape())
    if not messages:
        log.warning("No messages scraped")
        return

    df = pd.DataFrame(messages)

    df['city'] = df['text'].apply(extract_cities)
    df = df.explode('city')

    # Binary detection
    for name, pattern in THREAT_PATTERNS.items():
        df[name] = df['text'].str.contains(pattern, flags=re.IGNORECASE).astype(int)

    # All clear logic
    clear_mask = df['all_clear'] == 1
    df.loc[clear_mask, FEATURE_THREATS] = 0

    # Round to hour
    df['datetime'] = pd.to_datetime(df['datetime']).dt.floor('h')

    # IMPORTANT: BINARY AGGREGATION (NOT SUM)
    agg = df.groupby(['datetime', 'city'])[list(THREAT_PATTERNS.keys())].max().reset_index()

    # Load old data
    if RAW_FILE.exists():
        old = pd.read_csv(RAW_FILE)
        old['datetime'] = pd.to_datetime(old['datetime'], utc=True)
        agg = pd.concat([old, agg], ignore_index=True)

    # Remove duplicates
    agg = agg.drop_duplicates(['datetime', 'city'], keep='last')

    # Keep 48h
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=RAW_HISTORY_HOURS)
    agg = agg[agg['datetime'] >= cutoff]

    # Fill grid
    hours = pd.date_range(
        start=agg['datetime'].min(),
        end=now_utc,
        freq='h'
    )

    grid = pd.MultiIndex.from_product([hours, ALL_CITIES], names=['datetime', 'city'])
    full = pd.DataFrame(index=grid).reset_index()

    full = full.merge(agg, on=['datetime', 'city'], how='left')

    for col in THREAT_PATTERNS.keys():
        full[col] = full[col].fillna(0).astype(int)

    full = full.sort_values(['city', 'datetime']).reset_index(drop=True)

    RAW_FILE.parent.mkdir(parents=True, exist_ok=True)
    full.to_csv(RAW_FILE, index=False)

    log.info(f"Saved RAW: {RAW_FILE}")

# ── STAGE 2 ───────────────────────────────────────────────────────────────────

def generate_features():
    log.info("Generating FEATURES...")

    if not RAW_FILE.exists():
        log.error("RAW file missing")
        return

    df = pd.read_csv(RAW_FILE)
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)

    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    df = df.sort_values(['city', 'datetime'])

    # Create lags (binary stays binary)
    for t in FEATURE_THREATS:
        df[f'{t}_lag1'] = df.groupby('city')[t].shift(1)
        df[f'{t}_lag3'] = df.groupby('city')[t].shift(3)
        df[f'{t}_lag6'] = df.groupby('city')[t].shift(6)

    future_hours = pd.date_range(
        start=now_utc + timedelta(hours=1),
        periods=FORECAST_HOURS,
        freq='h'
    )

    rows = []

    for city in ALL_CITIES:
        city_hist = df[df['city'] == city]

        if city_hist.empty:
            continue

        for i, future_dt in enumerate(future_hours, start=1):

            row = {
                'datetime': future_dt,
                'region': city
            }

            for t in FEATURE_THREATS:

                # lag1
                if i == 1:
                    val = city_hist.loc[city_hist['datetime'] == now_utc, t].values
                    row[f'tg_{t}_count_lag1h'] = int(val[0]) if len(val) else 0
                else:
                    row[f'tg_{t}_count_lag1h'] = float('nan')

                # lag3
                if i <= 3:
                    lag_dt = now_utc - timedelta(hours=(3 - i))
                    val = city_hist.loc[city_hist['datetime'] == lag_dt, t].values
                    row[f'tg_{t}_count_lag3h'] = int(val[0]) if len(val) else 0
                else:
                    row[f'tg_{t}_count_lag3h'] = float('nan')

                # lag6
                if i <= 6:
                    lag_dt = now_utc - timedelta(hours=(6 - i))
                    val = city_hist.loc[city_hist['datetime'] == lag_dt, t].values
                    row[f'tg_{t}_count_lag6h'] = int(val[0]) if len(val) else 0
                else:
                    row[f'tg_{t}_count_lag6h'] = float('nan')

            rows.append(row)

    df_out = pd.DataFrame(rows)
    df_out = df_out.sort_values(['region', 'datetime']).reset_index(drop=True)

    FEATURES_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(FEATURES_FILE, index=False)

    log.info(f"Saved FEATURES: {FEATURES_FILE}")
    log.info(f"Shape: {df_out.shape}")

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    collect_raw_data()
    generate_features()

if __name__ == "__main__":
    main()