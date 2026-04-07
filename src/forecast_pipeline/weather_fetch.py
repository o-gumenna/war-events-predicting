"""
weather_forecast_features.py
────────────────────────────────────────────────────────────────────────────────
Script for automated weather data collection and feature generation
for the upcoming 24 hours.

Main execution steps:
  1. Retrieve 48-hour data for each city (24 hours past + 24 hours future).
  2. Calculate rolling features using historical data from the past 24 hours.
  3. Filter data to retain only forecast values for the next 24 hours.
  4. Deduplicate records and save the results in CSV format.
────────────────────────────────────────────────────────────────────────────────
"""

import os
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import time
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

OUTPUT_FILE = "data/weather/weather_features_hourly.csv"

# Dictionary for mapping API city names to standardized names
CITIES_MAPPING = {
    'Cherkasy': 'Cherkasy',
    'Chernihiv': 'Chernihiv',
    'Chernivtsi': 'Chernivtsi',
    'Dnipro': 'Dnipro',
    'Donetsk': 'Donetsk',
    'Ivano-Frankivsk': 'Ivano-Frankivsk',
    'Kharkiv': 'Kharkiv',
    'Kherson': 'Kherson',
    'Khmelnytskyi': 'Khmelnytskyi',
    'Kropyvnytskyi': 'Kropyvnytskyi',
    'Kyiv': 'Kyiv',
    'Lutsk': 'Lutsk',
    'Lviv': 'Lviv',
    'Mykolaiv': 'Mykolaiv',
    'Odesa': 'Odesa',
    'Poltava': 'Poltava',
    'Rivne': 'Rivne',
    'Sumy': 'Sumy',
    'Ternopil': 'Ternopil',
    'Uzhgorod': 'Uzhhorod',
    'Vinnytsia': 'Vinnytsia',
    'Zaporozhye': 'Zaporizhzhia',
    'Zhytomyr': 'Zhytomyr',
}

# API Interaction Module

def get_weather_48h(location: str, api_key: str) -> dict | None:
    """
    Retrieves weather data for 48 hours (previous and upcoming 24 hours).
    Returns the response in JSON format or None in case of a request error.
    """
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=24)).strftime("%Y-%m-%d")
    end = (now + timedelta(hours=24)).strftime("%Y-%m-%d")

    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{location}/{start}/{end}"
        f"?unitGroup=metric&key={api_key}&contentType=json&include=hours"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"  ERROR {location}: {e}")
        return None



# Data Parsing Module

def parse_hours(raw_data: dict, region_name: str) -> pd.DataFrame:
    """
    Transforms nested hourly data from JSON format into a flat DataFrame.
    Preserves general daily conditions and icons for further feature engineering.
    """
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
                # Preservation of text weather descriptions for subsequent classification
                "conditions": hour.get("conditions", ""),
                "day_conditions": day_conditions,
                "icon": hour.get("icon", day_icon),
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime_epoch"], unit="s", utc=True).dt.floor("h")
    df = df.drop(columns=["datetime_epoch"])
    df = df.sort_values("datetime").reset_index(drop=True)
    return df


# Feature Engineering Module


def check_weather(df: pd.DataFrame, keywords: list[str]) -> pd.Series:
    """
    Checks for the presence of keywords in text description columns.
    Used to create binary features for specific weather conditions.
    """
    pattern = "|".join(keywords)
    target_cols = ["conditions", "day_conditions", "icon"]
    return (
        df[target_cols]
        .apply(lambda x: x.str.contains(pattern, case=False, na=False))
        .any(axis=1)
        .astype(int)
    )

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates new engineered features based on raw weather data.
    For correct calculation of rolling windows, the DataFrame must
    contain data for at least the past 24 hours.
    """
    df = df.sort_values(["city", "datetime"]).reset_index(drop=True)

    # Aggregation of precipitation over the last 24 hours (rolling window)
    df["rolling_precip_24h"] = (
        df.groupby("city", observed=True)["precip"]
        .transform(lambda x: x.rolling(24, min_periods=1).sum())
    )

    # Transformation of wind direction into sinusoidal form to account for cyclicity
    df["winddir_sin"] = np.sin(np.deg2rad(df["winddir"]))

    # Creation of binary indicators for specific weather phenomena
    df["is_snow"] = check_weather(df, ["Snow", "Ice"])
    df["is_rain"] = check_weather(df, ["Rain", "Drizzle"])
    df["is_clear"] = (
            check_weather(df, ["Clear", "clear-day"]) &
            (df["is_snow"] == 0) &
            (df["is_rain"] == 0)
    ).astype(int)

    return df


# Main Execution

def main():
    print("=" * 60)
    print("  WEATHER FEATURES COLLECTION AND GENERATION")
    print("=" * 60)

    # Check for the presence of the API access key
    if not WEATHER_API_KEY:
        print("ERROR: API_KEY is not configured.")
        return

    # Definition of time boundaries for the forecast (from T+1 to T+24 hours)
    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    forecast_start = now_utc + timedelta(hours=1)
    forecast_end = now_utc + timedelta(hours=24)

    all_regions = []

    for api_location, region_name in CITIES_MAPPING.items():
        print(f"  {region_name:20} ... ", end="", flush=True)

        raw = get_weather_48h(api_location, WEATHER_API_KEY)
        time.sleep(1.5)

        if raw is None:
            print("SKIP")
            continue

        df = parse_hours(raw, region_name)
        if df.empty:
            print("EMPTY")
            continue

        df = engineer_features(df)

        # Filtering: retaining only the target forecast hours
        df_forecast = df[
            (df["datetime"] >= forecast_start) &
            (df["datetime"] <= forecast_end)
        ].copy()

        if df_forecast.empty:
            print("NO FORECAST HOURS")
            continue

        all_regions.append(df_forecast)
        print(f"OK ({len(df_forecast)} rows)")

    if not all_regions:
        print("\nFATAL: No data collected.")
        return

    # Merging results and removing potential duplicates
    result = pd.concat(all_regions, ignore_index=True)
    before = len(result)
    result = result.drop_duplicates(subset=["datetime", "city"])
    after = len(result)
    if before != after:
        print(f"\nDeduplicated: {before - after} duplicate rows removed")

    # Formation of the final set of target columns
    final_cols = [
        "datetime", "city",
        "humidity", "dew", "pressure", "cloudcover", "winddir",
        "rolling_precip_24h", "winddir_sin",
        "is_snow", "is_rain", "is_clear",
    ]
    result = result[final_cols].sort_values(["city", "datetime"]).reset_index(drop=True)

    # Exporting results to CSV
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    result.to_csv(OUTPUT_FILE, index=False)

    print()
    print("=" * 60)
    print(f"Saved: {OUTPUT_FILE}")
    print(f"Shape: {result.shape}")
    print(f"Cities: {sorted(result['city'].unique().tolist())}")
    print(f"Hours:  {result['datetime'].min()} → {result['datetime'].max()}")
    print("=" * 60)

if __name__ == "__main__":
    main()