import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = os.path.dirname(__file__)
INPUT_PATH  = os.path.join(BASE_DIR, "data", "alarms", "alarms_raw.json")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "alarms", "alarms_features.csv")

ALL_REGIONS = [
    "Cherkasy", "Chernihiv", "Chernivtsi", "Dnipro", "Donetsk",
    "Ivano-Frankivsk", "Kharkiv", "Kherson", "Khmelnytskyi", "Kropyvnytskyi",
    "Kyiv", "Lutsk", "Lviv", "Mykolaiv", "Odesa", "Poltava", "Rivne",
    "Sumy", "Ternopil", "Uzhhorod", "Vinnytsia", "Zaporizhzhia", "Zhytomyr"
]


def load_raw_snapshots() -> pd.DataFrame:
    """
    Читає alarms_raw.json і будує погодинну таблицю.
    Кожен snapshot = один рядок на кожен регіон.
    """
    path = Path(INPUT_PATH)
    if not path.exists():
        print(f"File not found: {INPUT_PATH}")
        return pd.DataFrame()

    with open(path, "r", encoding="utf-8") as f:
        records = json.load(f)

    print(f"Found {len(records)} snapshots")

    rows = []
    for snapshot in records:
        collected_at   = pd.to_datetime(snapshot["collected_at"]).tz_convert("UTC")
        active_regions = set(snapshot.get("active_regions", []))

        for region in ALL_REGIONS:
            rows.append({
                "datetime": collected_at,
                "region":   region,
                "alarm":    1 if region in active_regions else 0,
            })

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["datetime", "region"])
    df = df.sort_values(["region", "datetime"]).reset_index(drop=True)

    return df


def build_full_hourly_grid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Будує повну погодинну сітку як в notebook:
    від мінімальної до максимальної дати, кожна година, кожен регіон.
    Якщо в якийсь час не було snapshot — заповнює попереднім значенням (ffill).

    Це відповідає логіці з notebook:
    full_grid = pd.MultiIndex.from_product([all_regions, full_range])
    """
    if df.empty:
        return df

    # будуємо повну погодинну сітку
    full_range = pd.date_range(
        start=df["datetime"].min().floor("h"),
        end=df["datetime"].max().ceil("h"),
        freq="h",
        tz="UTC"
    )

    full_grid = pd.MultiIndex.from_product(
        [ALL_REGIONS, full_range],
        names=["region", "datetime"]
    )

    df_grid = pd.DataFrame(index=full_grid).reset_index()

    # мерджимо реальні дані на сітку
    # округлюємо datetime до години для точного мерджу
    df["datetime_h"] = df["datetime"].dt.floor("h")

    df_merged = df_grid.merge(
        df[["datetime_h", "region", "alarm"]].rename(columns={"datetime_h": "datetime"}),
        on=["region", "datetime"],
        how="left"
    )

    # якщо в якусь годину не було snapshot — беремо попереднє значення
    df_merged["alarm"] = (
        df_merged.groupby("region")["alarm"]
        .transform(lambda x: x.ffill().fillna(0))
        .astype(int)
    )

    return df_merged


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Рахує всі фічі точно як в notebook war_events-2.ipynb:

    1. alarm_lag_1h/3h/6h/24h — shift по групі region
    2. active_regions_count    — скільки регіонів з тривогою в цей час
    3. alarm_hours_last_24h    — rolling('24h') по часовому індексу
    4. alarm_events_last_24h   — кількість початків тривоги за 24г
    """
    df = df.sort_values(["region", "datetime"]).reset_index(drop=True)

    # 1. лаги — точно як в Cell 108 notebook
# 1. Головне зміщення: тепер колонка 'alarm' — це стан 1 годину тому
    df["alarm"] = (
    df.groupby("region")["alarm"]
    .shift(1)
    .fillna(0)
    .astype(int)
    )

# 2. Інші лаги рахуються ВІД ВЖЕ ЗМІЩЕНОГО alarm
# Наприклад: alarm(t-1) .shift(2) = стан за t-3
    lags_to_compute = [
    ("alarm_lag_3h", 2), 
    ("alarm_lag_6h", 5), 
    ("alarm_lag_24h", 23)
    ]

    for lag_name, lag_val in lags_to_compute:
     df[lag_name] = (
        df.groupby("region")["alarm"]
        .shift(lag_val)
        .fillna(0)
        .astype(int)
     )

    # 2. active_regions_count — скільки регіонів з тривогою в кожну годину
    # точно як в Cell 102 notebook
    active_per_hour = (
        df[df["alarm"] == 1]
        .groupby("datetime")["region"]
        .nunique()
        .reset_index()
        .rename(columns={"region": "active_regions_count"})
    )
    df = df.merge(active_per_hour, on="datetime", how="left")
    df["active_regions_count"] = df["active_regions_count"].fillna(0).astype(int)

    # 3. alarm_hours_last_24h — rolling по часовому індексу
    # точно як в Cell 124 notebook
    df = df.sort_values(["region", "datetime"]).reset_index(drop=True)
    df.index = pd.DatetimeIndex(df["datetime"])

    df["alarm_hours_last_24h"] = (
        df.groupby("region")["alarm"]
        .transform(lambda x: x.rolling("24h", closed="left").sum())
    ).values

    df = df.reset_index(drop=True)
    df["alarm_hours_last_24h"] = df["alarm_hours_last_24h"].fillna(0).astype(int)

    # 4. alarm_events_last_24h — кількість початків тривоги за 24г
    # точно як в Cell 122 notebook
    df["alarm_start"] = (
        df.groupby("region")["alarm"]
        .transform(lambda x: ((x == 1) & (x.shift(1) == 0)).astype(int))
    )

    df = df.sort_values(["region", "datetime"]).reset_index(drop=True)
    df.index = pd.DatetimeIndex(df["datetime"])

    df["alarm_events_last_24h"] = (
        df.groupby("region")["alarm_start"]
        .transform(lambda x: x.rolling("24h", closed="left").sum())
    ).values

    df = df.reset_index(drop=True)
    df["alarm_events_last_24h"] = df["alarm_events_last_24h"].fillna(0).astype(int)
    df = df.drop(columns=["alarm_start"])

    # фінальні колонки — точно як в Cell 128 notebook
    df = df.rename(columns={"region": "city"})
    df = df[[
    "datetime", "city", "alarm",
    "active_regions_count",
    "alarm_hours_last_24h",
    "alarm_events_last_24h",
    "alarm_lag_3h",
    "alarm_lag_6h", "alarm_lag_24h"
    ]]
    return df


def save_features(df: pd.DataFrame):
    path = Path(OUTPUT_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved {len(df)} rows to {OUTPUT_PATH}")


def process_alarms():
    print(f"Processing alarms at {datetime.now(timezone.utc).isoformat()}...")

    # крок 1 — завантажуємо сирі дані
    df = load_raw_snapshots()
    if df.empty:
        print("No data to process!")
        return

    print(f"Raw rows: {len(df)}")
    print(f"Date range: {df['datetime'].min()} → {df['datetime'].max()}")

    # крок 2 — будуємо повну погодинну сітку
    df = build_full_hourly_grid(df)
    print(f"After hourly grid: {len(df)} rows")

    # крок 3 — рахуємо фічі
    df = compute_features(df)

    # перевірка активних тривог в останньому snapshot

    last_known_time = df["datetime"].max()

    # Відбираємо тільки найсвіжіший стан для кожного міста (останні відомі лаги)
    latest_state = df[df["datetime"] == last_known_time].copy()

    # Генеруємо 24 майбутні години (від T+1 до T+24)
    forecast_rows = []
    for i in range(1, 25):
        future_time = last_known_time + pd.Timedelta(hours=i)

        # Беремо останній стан, але змінюємо йому час на майбутній
        future_df = latest_state.copy()
        future_df["datetime"] = future_time
        forecast_rows.append(future_df)

    # Зліплюємо 24 години в один датафрейм
    df_forecast = pd.concat(forecast_rows, ignore_index=True)

    # Сортуємо для краси
    df_forecast = df_forecast.sort_values(["city", "datetime"]).reset_index(drop=True)

    print(f"\nGenerated {len(df_forecast)} forecast rows (24h for {len(ALL_REGIONS)} cities).")

    # крок 5 — зберігаємо ТІЛЬКИ майбутні 24 години для merging.py
    save_features(df_forecast)

if __name__ == "__main__":
    process_alarms()
