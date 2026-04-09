"""
merge_features.py
────────────────────────────────────────────────────────────────────────────────
Збирає всі підготовлені фічі та зливає їх в один датасет.
────────────────────────────────────────────────────────────────────────────────
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("merge_pipeline")

BASE_DIR = Path(__file__).resolve().parent.parent.parent

FILE_ALARMS = BASE_DIR / "data" / "alarms" / "alarms_features_hourly.csv"
FILE_ISW = BASE_DIR / "data" / "isw" / "isw_forecast_features.csv"
FILE_TELEGRAM = BASE_DIR / "data" / "telegram" / "telegram_features_24h.csv"
FILE_WEATHER = BASE_DIR / "data" / "weather" / "weather_features_hourly.csv"

OUTPUT_DIR = BASE_DIR / "data" / "final"
OUTPUT_FILE = OUTPUT_DIR / "FINAL_FEATURES_24H.csv"


def load_and_prep(filepath: Path, date_col='datetime') -> pd.DataFrame | None:
    if not filepath.exists():
        log.warning(f"Файл не знайдено: {filepath.name}. Пропускаємо...")
        return None
    try:
        df = pd.read_csv(filepath)
        df[date_col] = pd.to_datetime(df[date_col], format='mixed', utc=True).dt.tz_localize(None)
        return df
    except Exception as e:
        log.error(f"Помилка читання {filepath.name}: {e}")
        return None


def main():
    log.info("=== Початок злиття фічей (Merge) ===")

    # 1. Тривоги (База)
    df_base = load_and_prep(FILE_ALARMS)
    if df_base is None or df_base.empty:
        return
    if 'city' in df_base.columns:
        df_base = df_base.rename(columns={'city': 'region'})

    # 2. Погода
    df_weather = load_and_prep(FILE_WEATHER)
    if df_weather is not None and not df_weather.empty:
        if 'city' in df_weather.columns:
            df_weather = df_weather.rename(columns={'city': 'region'})
        df_base = pd.merge(df_base, df_weather, on=['datetime', 'region'], how='left')

    # 3. Телеграм (Просто склеюємо, залишаємо NaN на своїх місцях)
    df_tg = load_and_prep(FILE_TELEGRAM)
    if df_tg is not None and not df_tg.empty:
        if 'city' in df_tg.columns:
            df_tg = df_tg.rename(columns={'city': 'region'})
        df_base = pd.merge(df_base, df_tg, on=['datetime', 'region'], how='left')

    # 4. ISW (Просто склеюємо)
    df_isw = load_and_prep(FILE_ISW)
    if df_isw is not None and not df_isw.empty:
        df_base = pd.merge(df_base, df_isw, on='datetime', how='left')

    # 5. Імітація Reddit (оскільки ми його не збираємо, віддаємо моделі нулі = "немає постів")
    reddit_features = ['avg_comments', 'avg_upvote_ratio', 'reddit_city_count']
    for col in reddit_features:
        df_base[col] = 0.0

    # 6. One-Hot Encoding для міст (обов'язково для моделі)
    df_base['city_for_ohe'] = df_base['region']
    df_base = pd.get_dummies(df_base, columns=['city_for_ohe'], prefix='city', dtype=int)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_base.to_csv(OUTPUT_FILE, index=False)

    log.info(f"=== Злиття завершено! Збережено у {OUTPUT_FILE.name} ===")


if __name__ == "__main__":
    main()