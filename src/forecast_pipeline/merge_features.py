"""
merge_features.py
────────────────────────────────────────────────────────────────────────────────
Збирає всі підготовлені фічі та зливає їх в один датасет.

Стандарт datetime: tz-aware UTC (pandas DatetimeTZDtype[ns, UTC]).
Всі джерела приводяться до цього стандарту в load_and_prep().
────────────────────────────────────────────────────────────────────────────────
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
    """Читає CSV і нормалізує datetime до tz-aware UTC."""
    if not filepath.exists():
        log.warning(f"Файл не знайдено: {filepath.name}. Пропускаємо...")
        return None
    try:
        df = pd.read_csv(filepath)
        # pd.to_datetime(..., utc=True) коректно обробляє:
        #   - рядки з "+00:00" (ISW, weather)
        #   - naive рядки (alarms, telegram) — інтерпретує як UTC
        df[date_col] = pd.to_datetime(df[date_col], utc=True)
        return df
    except Exception as e:
        log.error(f"Помилка читання {filepath.name}: {e}")
        return None


def main():
    log.info("=== Початок злиття фічей (Merge) ===")

    # 1. Тривоги (База)
    df_base = load_and_prep(FILE_ALARMS)
    if df_base is None or df_base.empty:
        log.error("Файл тривог відсутній або порожній. Перервано.")
        return
    if 'city' in df_base.columns:
        df_base = df_base.rename(columns={'city': 'region'})
    log.info(f"Alarms: {df_base.shape[0]} rows, {df_base['datetime'].min()} → {df_base['datetime'].max()}")

    # 2. Погода
    df_weather = load_and_prep(FILE_WEATHER)
    if df_weather is not None and not df_weather.empty:
        if 'city' in df_weather.columns:
            df_weather = df_weather.rename(columns={'city': 'region'})
        df_base = pd.merge(df_base, df_weather, on=['datetime', 'region'], how='left')
        log.info(f"After weather merge: {df_base.shape}")
    else:
        log.warning("Погоду пропущено — файл відсутній.")

    # 3. Телеграм
    df_tg = load_and_prep(FILE_TELEGRAM)
    if df_tg is not None and not df_tg.empty:
        if 'city' in df_tg.columns:
            df_tg = df_tg.rename(columns={'city': 'region'})
        df_base = pd.merge(df_base, df_tg, on=['datetime', 'region'], how='left')
        log.info(f"After telegram merge: {df_base.shape}")
    else:
        log.warning("Telegram пропущено — файл відсутній.")

    # 4. ISW — merge тільки по datetime (одна строка на годину, без регіону)
    df_isw = load_and_prep(FILE_ISW)
    if df_isw is not None and not df_isw.empty:
        # Видаляємо isw_<region> колонки з ISW перед merge —
        # вони не потрібні як окремі фічі для моделі (є is_city_in_isw)
        isw_region_cols = [c for c in df_isw.columns if c.startswith("isw_") and
                           any(c == f"isw_{r}" for r in ALL_REGIONS)]
        df_isw_merge = df_isw.drop(columns=isw_region_cols, errors='ignore')
        df_base = pd.merge(df_base, df_isw_merge, on='datetime', how='left')

        # 4a. is_city_in_isw: чи згадується місто у звіті ISW
        # ISW-файл містить isw_<Region> = 1/0 для кожного регіону
        df_base['is_city_in_isw'] = 0
        for region in ALL_REGIONS:
            isw_col = f'isw_{region}'
            if isw_col in df_isw.columns:
                # map datetime → isw_col value, then assign per-row
                isw_lookup = df_isw.set_index('datetime')[isw_col]
                mask = df_base['region'] == region
                df_base.loc[mask, 'is_city_in_isw'] = (
                    df_base.loc[mask, 'datetime'].map(isw_lookup).fillna(0).astype(int)
                )
        log.info(f"After ISW merge: {df_base.shape}")
    else:
        log.warning("ISW пропущено — файл відсутній.")
        df_base['is_city_in_isw'] = 0

    # 5. Reddit-заглушки (не збираємо в realtime — даємо моделі нулі)
    for col in ['avg_comments', 'avg_upvote_ratio', 'reddit_city_count', 'lda_info_war', 'lda_politics_crimes']:
        df_base[col] = 0.0

    # 6. One-Hot Encoding для міст (префікс city_, без drop_first щоб усі 23 міста)
    df_base['city_for_ohe'] = df_base['region']
    df_base = pd.get_dummies(df_base, columns=['city_for_ohe'], prefix='city', dtype=int)

    # Гарантуємо що всі 23 OHE-колонки присутні (можуть відсутні якщо регіон не з'явився)
    for region in ALL_REGIONS:
        col = f'city_{region}'
        if col not in df_base.columns:
            df_base[col] = 0

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_base.to_csv(OUTPUT_FILE, index=False)

    log.info(f"Shape: {df_base.shape}")
    log.info(f"Datetime range: {df_base['datetime'].min()} → {df_base['datetime'].max()}")
    log.info(f"=== Злиття завершено! Збережено у {OUTPUT_FILE.name} ===")


if __name__ == "__main__":
    main()