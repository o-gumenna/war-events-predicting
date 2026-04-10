"""
weather_forecast_features.py
────────────────────────────────────────────────────────────────────────────────
Script for automated weather data collection and feature generation.
OPTIMIZED: Uses local history caching to minimize API costs.

Main execution steps:
  1. Load local historical weather data (past 48 hours).
  2. Fetch standard API forecast (costs only 1 record per city).
  3. Append newly observed actuals (current hour) to local history.
  4. Combine local history + future forecast to calculate 24h rolling features.
  5. Filter data to retain only forecast values for the next 24 hours.
  6. Save the final features and update the local history file.
────────────────────────────────────────────────────────────────────────────────
"""

import os
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import time
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

HISTORY_FILE = "data/weather/weather_history.csv"
OUTPUT_FILE = "data/weather/weather_features_hourly.csv"

CITIES_MAPPING = {
    'Cherkasy': 'Cherkasy', 'Chernihiv': 'Chernihiv', 'Chernivtsi': 'Chernivtsi',
    'Dnipro': 'Dnipro', 'Donetsk': 'Donetsk', 'Ivano-Frankivsk': 'Ivano-Frankivsk',
    'Kharkiv': 'Kharkiv', 'Kherson': 'Kherson', 'Khmelnytskyi': 'Khmelnytskyi',
    'Kropyvnytskyi': 'Kropyvnytskyi', 'Kyiv': 'Kyiv', 'Lutsk': 'Lutsk',
    'Lviv': 'Lviv', 'Mykolaiv': 'Mykolaiv', 'Odesa': 'Odesa',
    'Poltava': 'Poltava', 'Rivne': 'Rivne', 'Sumy': 'Sumy',
    'Ternopil': 'Ternopil', 'Uzhgorod': 'Uzhhorod', 'Vinnytsia': 'Vinnytsia',
    'Zaporozhye': 'Zaporizhzhia', 'Zhytomyr': 'Zhytomyr',
}


def get_weather_forecast(location: str, api_key: str) -> dict | None:
    """
    Retrieves standard forecast (today + 14 days).
    Cost: Exactly 1 record per location.
    """
    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{location}"
        f"?unitGroup=metric&key={api_key}&contentType=json&include=hours"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"  ERROR {location}: {e}")
        return None


def parse_hours(raw_data: dict, region_name: str) -> pd.DataFrame:
    rows = []
    for day in raw_data.get("days", []):
        day_conditions = day.get("conditions", "")
        day_icon = day.get("icon", "")

        for hour in day.get("hours", []):
            rows.append({
                "datetime_epoch": hour.get("datetimeEpoch"),
                "city": region_name,
                "humidity": hour.get("humidity", 0.0),
                "dew": hour.get("dew", 0.0),
                "pressure": hour.get("pressure", 0.0),
                "cloudcover": hour.get("cloudcover", 0.0),
                "winddir": hour.get("winddir", 0.0),
                "precip": hour.get("precip", 0.0),
                "conditions": hour.get("conditions", ""),
                "day_conditions": day_conditions,
                "icon": hour.get("icon", day_icon),
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime_epoch"], unit="s", utc=True).dt.floor("h")
    df = df.drop(columns=["datetime_epoch"])
    return df


def check_weather(df: pd.DataFrame, keywords: list[str]) -> pd.Series:
    pattern = "|".join(keywords)
    target_cols = ["conditions", "day_conditions", "icon"]
    return (
        df[target_cols]
        .apply(lambda x: x.str.contains(pattern, case=False, na=False))
        .any(axis=1)
        .astype(int)
    )


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["city", "datetime"]).reset_index(drop=True)

    df["rolling_precip_24h"] = (
        df.groupby("city", observed=True)["precip"]
        .transform(lambda x: x.rolling(24, min_periods=1).sum())
    )

    df["winddir_sin"] = np.sin(np.deg2rad(df["winddir"]))
    df["is_snow"] = check_weather(df, ["Snow", "Ice"])
    df["is_rain"] = check_weather(df, ["Rain", "Drizzle"])
    df["is_clear"] = (
            check_weather(df, ["Clear", "clear-day"]) &
            (df["is_snow"] == 0) &
            (df["is_rain"] == 0)
    ).astype(int)

    return df


def main():
    print("=" * 60)
    print("OPTIMIZED WEATHER COLLECTION (LOCAL CACHE)")
    print("=" * 60)

    if not WEATHER_API_KEY:
        print("ERROR: API_KEY is not configured.")
        return

    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    
    # T = остання ЗАКРИТА година (див. process_alarms_final.py)
    # forecast: T+1..T+24 — синхронізовано між усіма скриптами
    T = now_utc - timedelta(hours=1)
    forecast_start = T + timedelta(hours=1)
    forecast_end   = T + timedelta(hours=24)
    history_cutoff = T - timedelta(hours=48)  # Keep 48h of history max

    # 1. Load Local History
    hist_path = Path(HISTORY_FILE)
    if hist_path.exists():
        df_hist = pd.read_csv(hist_path)
        # utc=True коректно обробляє і "+00:00" рядки, і naive рядки
        df_hist["datetime"] = pd.to_datetime(df_hist["datetime"], utc=True)
        print(f"Loaded local history: {len(df_hist)} rows.")
    else:
        df_hist = pd.DataFrame()
        print("No local history found. It will be created now.")

    all_fetched = []

    # 2. Fetch New Data
    for api_location, region_name in CITIES_MAPPING.items():
        print(f"  {region_name:20} ... ", end="", flush=True)

        raw = get_weather_forecast(api_location, WEATHER_API_KEY)
        time.sleep(3)  # Safe delay to avoid Burst Limits

        if raw is None:
            print("SKIP")
            continue

        df_city = parse_hours(raw, region_name)
        if df_city.empty:
            print("EMPTY")
            continue

        all_fetched.append(df_city)
        print("OK")

    if not all_fetched:
        print("\nFATAL: No data collected.")
        return

    df_fetched = pd.concat(all_fetched, ignore_index=True)

    # 3. Split Fetched Data into Past/Current and Future
    df_fetched_past = df_fetched[df_fetched["datetime"] <= now_utc].copy()
    df_fetched_future = df_fetched[df_fetched["datetime"] > now_utc].copy()

    # 4. Update Local History (Append new past hours, drop duplicates, prune old data)
    df_new_hist = pd.concat([df_hist, df_fetched_past], ignore_index=True)
    df_new_hist = df_new_hist.drop_duplicates(subset=["city", "datetime"], keep="last")
    df_new_hist = df_new_hist[df_new_hist["datetime"] >= history_cutoff].copy()

    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    df_new_hist.to_csv(HISTORY_FILE, index=False)
    print(f"\nUpdated local history saved: {len(df_new_hist)} rows.")

    # 5. Combine History + Future to Calculate Rolling Features safely
    df_full = pd.concat([df_new_hist, df_fetched_future], ignore_index=True)
    df_full = df_full.drop_duplicates(subset=["city", "datetime"], keep="last")

    df_features = engineer_features(df_full)

    # 6. Filter exactly T+1 to T+24 for output
    df_forecast = df_features[
        (df_features["datetime"] >= forecast_start) &
        (df_features["datetime"] <= forecast_end)
        ].copy()

    final_cols = [
        "datetime", "city",
        "humidity", "dew", "pressure", "cloudcover", "winddir",
        "rolling_precip_24h", "winddir_sin",
        "is_snow", "is_rain", "is_clear",
    ]
    df_forecast = df_forecast[final_cols].sort_values(["city", "datetime"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df_forecast.to_csv(OUTPUT_FILE, index=False)

    print("=" * 60)
    print(f"Saved Forecast: {OUTPUT_FILE}")
    print(f"Shape: {df_forecast.shape} (Expected: {23 * 24} rows)")
    print(f"Hours:  {df_forecast['datetime'].min()} → {df_forecast['datetime'].max()}")
    print("=" * 60)


if __name__ == "__main__":
    main()