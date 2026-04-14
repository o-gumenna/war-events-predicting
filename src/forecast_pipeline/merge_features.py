"""
merge_features.py

Combine all prepared feature tables into one final dataset.

Datetime handling is standardized as tz-aware UTC.
Each source is normalized to that format in load_and_prep().
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("merge_pipeline")

BASE_DIR = Path(__file__).resolve().parent.parent.parent

FILE_ALARMS  = BASE_DIR / "data" / "alarms"   / "alarms_features_hourly.csv"
FILE_ISW     = BASE_DIR / "data" / "isw"       / "isw_forecast_features.csv"
FILE_TELEGRAM = BASE_DIR / "data" / "telegram" / "telegram_features_24h.csv"
FILE_WEATHER = BASE_DIR / "data" / "weather"   / "weather_features_hourly.csv"

OUTPUT_DIR  = BASE_DIR / "data" / "final"
OUTPUT_FILE = OUTPUT_DIR / "FINAL_FEATURES_24H.csv"

ALL_REGIONS = [
    "Cherkasy", "Chernihiv", "Chernivtsi", "Dnipro", "Donetsk",
    "Ivano-Frankivsk", "Kharkiv", "Kherson", "Khmelnytskyi", "Kropyvnytskyi",
    "Kyiv", "Lutsk", "Lviv", "Mykolaiv", "Odesa", "Poltava", "Rivne",
    "Sumy", "Ternopil", "Uzhhorod", "Vinnytsia", "Zaporizhzhia", "Zhytomyr"
]


def load_and_prep(filepath: Path, date_col: str = 'datetime') -> pd.DataFrame | None:
    """Read a CSV file and normalize its datetime column to tz-aware UTC."""
    if not filepath.exists():
        log.warning(f"File not found: {filepath.name}. Skipping...")
        return None
    try:
        df = pd.read_csv(filepath)
        # utc=True keeps all sources on the same timeline,
        # including offset-aware and naive timestamp strings.
        df[date_col] = pd.to_datetime(df[date_col], utc=True)
        return df
    except Exception as e:
        log.error(f"Failed to read {filepath.name}: {e}")
        return None


def main():
    log.info("=== Starting feature merge ===")

    # 1. Alarms are the base table.
    df_base = load_and_prep(FILE_ALARMS)
    if df_base is None or df_base.empty:
        log.error("Alarm feature file is missing or empty. Stopping.")
        return
    if 'city' in df_base.columns:
        df_base = df_base.rename(columns={'city': 'region'})
    log.info(f"Alarms: {df_base.shape[0]} rows, {df_base['datetime'].min()} → {df_base['datetime'].max()}")

    # 2. Merge weather features by datetime and region.
    df_weather = load_and_prep(FILE_WEATHER)
    if df_weather is not None and not df_weather.empty:
        if 'city' in df_weather.columns:
            df_weather = df_weather.rename(columns={'city': 'region'})
        df_base = pd.merge(df_base, df_weather, on=['datetime', 'region'], how='left')
        log.info(f"After weather merge: {df_base.shape}")
    else:
        log.warning("Weather features skipped because the file is missing.")

    # 3. Merge Telegram features by datetime and region.
    df_tg = load_and_prep(FILE_TELEGRAM)
    if df_tg is not None and not df_tg.empty:
        if 'city' in df_tg.columns:
            df_tg = df_tg.rename(columns={'city': 'region'})
        df_base = pd.merge(df_base, df_tg, on=['datetime', 'region'], how='left')
        log.info(f"After telegram merge: {df_base.shape}")
    else:
        log.warning("Telegram features skipped because the file is missing.")

    # 4. Merge ISW features by datetime only.
    # ISW is a report-level signal, not a city-level table.
    df_isw = load_and_prep(FILE_ISW)
    if df_isw is not None and not df_isw.empty:
        # Drop region flags before the merge.
        # They are reused below to build the city-specific indicator.
        isw_region_cols = [c for c in df_isw.columns if c.startswith("isw_") and
                           any(c == f"isw_{r}" for r in ALL_REGIONS)]
        df_isw_merge = df_isw.drop(columns=isw_region_cols, errors='ignore')
        df_base = pd.merge(df_base, df_isw_merge, on='datetime', how='left')

        # 4a. Build is_city_in_isw from the original per-region ISW flags.
        df_base['is_city_in_isw'] = 0
        for region in ALL_REGIONS:
            isw_col = f'isw_{region}'
            if isw_col in df_isw.columns:
                # Map each timestamp to the region flag and assign it row by row.
                isw_lookup = df_isw.set_index('datetime')[isw_col]
                mask = df_base['region'] == region
                df_base.loc[mask, 'is_city_in_isw'] = (
                    df_base.loc[mask, 'datetime'].map(isw_lookup).fillna(0).astype(int)
                )
        log.info(f"After ISW merge: {df_base.shape}")
    else:
        log.warning("ISW features skipped because the file is missing.")
        df_base['is_city_in_isw'] = 0

    # 5. Reddit features are filled with zeros because they are not collected in realtime.
    for col in ['avg_comments', 'avg_upvote_ratio', 'reddit_city_count', 'lda_info_war', 'lda_politics_crimes']:
        df_base[col] = 0.0

    # 6. Add full one-hot encoding for all modeled cities.
    df_base['city_for_ohe'] = df_base['region']
    df_base = pd.get_dummies(df_base, columns=['city_for_ohe'], prefix='city', dtype=int)

    # Ensure that every expected city column exists, even if a region is absent in this batch.
    for region in ALL_REGIONS:
        col = f'city_{region}'
        if col not in df_base.columns:
            df_base[col] = 0

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_base.to_csv(OUTPUT_FILE, index=False)

    log.info(f"Shape: {df_base.shape}")
    log.info(f"Datetime range: {df_base['datetime'].min()} → {df_base['datetime'].max()}")
    log.info(f"=== Feature merge finished. Saved to {OUTPUT_FILE.name} ===")


if __name__ == "__main__":
    main()
