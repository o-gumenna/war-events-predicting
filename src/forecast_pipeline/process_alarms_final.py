import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path

INPUT_PATH = "data/alarms/alarms_history_raw.csv"
OUTPUT_PATH = "data/alarms/alarms_features_hourly.csv"

ALL_REGIONS = [
    "Cherkasy", "Chernihiv", "Chernivtsi", "Dnipro", "Donetsk",
    "Ivano-Frankivsk", "Kharkiv", "Kherson", "Khmelnytskyi", "Kropyvnytskyi",
    "Kyiv", "Lutsk", "Lviv", "Mykolaiv", "Odesa", "Poltava", "Rivne",
    "Sumy", "Ternopil", "Uzhhorod", "Vinnytsia", "Zaporizhzhia", "Zhytomyr"
]


def process_alarms():
    # Use tz-aware UTC throughout the pipeline.
    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    print(f"[{now_utc.isoformat()}] Processing hourly features...")

    path = Path(INPUT_PATH)
    if not path.exists():
        print(f"No raw history found at {INPUT_PATH}. Wait for collector to run.")
        return

    # 1. Load 5-minute raw alarm snapshots.
    df_raw = pd.read_csv(path)
    # utc=True keeps mixed timestamp formats on a single UTC timeline.
    df_raw["datetime"] = pd.to_datetime(df_raw["datetime"], utc=True)
    df_raw = df_raw.sort_values(["city", "datetime"]).reset_index(drop=True)

    # 2. Count alarm start events at the 5-minute level.
    df_raw["alarm_start_5min"] = df_raw.groupby("city")["alarm"].transform(
        lambda x: ((x == 1) & (x.shift(1).fillna(0) == 0)).astype(int)
    )

    # 3. Aggregate the raw stream into hourly windows.
    df_raw["datetime_h"] = df_raw["datetime"].dt.floor("h")

    # A. Alarm presence in the hour: 1 if it happened at least once.
    df_hourly_alarm = df_raw.groupby(["city", "datetime_h"])["alarm"].max().reset_index()

    # B. Count how many times the siren started in that hour.
    df_hourly_events = df_raw.groupby(["city", "datetime_h"])["alarm_start_5min"].sum().reset_index()
    df_hourly_events.rename(columns={"alarm_start_5min": "events_in_hour"}, inplace=True)

    # Combine hourly alarm presence with hourly event counts.
    df = df_hourly_alarm.merge(df_hourly_events, on=["city", "datetime_h"])
    df.rename(columns={"datetime_h": "datetime"}, inplace=True)

    # 4. Define the reference window.
    # now_utc is the current rounded hour.
    # T is the last fully closed hour.
    # The forecast window is T+1..T+24.
    T = now_utc - timedelta(hours=1)

    forecast_start = T + timedelta(hours=1)  # = now_utc
    forecast_end   = T + timedelta(hours=24)

    # 5. Build the full grid: history plus the next 24 forecast hours.
    full_range = pd.date_range(start=df["datetime"].min(), end=forecast_end, freq="h", tz="UTC")
    full_grid = pd.MultiIndex.from_product([ALL_REGIONS, full_range], names=["city", "datetime"])
    df_grid = pd.DataFrame(index=full_grid).reset_index()

    df = df_grid.merge(df, on=["city", "datetime"], how="left")

    # Count active regions for each hour across the whole country.
    # min_count=1 keeps future rows as NaN instead of turning them into zero.
    active_per_hour = df.groupby("datetime")["alarm"].sum(min_count=1).reset_index()
    active_per_hour.rename(columns={"alarm": "active_regions_count"}, inplace=True)
    df = df.merge(active_per_hour, on="datetime", how="left")

    df.loc[df["datetime"] > T, ["alarm", "events_in_hour", "active_regions_count"]] = float('nan')
    df = df.sort_values(["city", "datetime"]).reset_index(drop=True)

    # 6. Compute lag and rolling features without forcing zero-fill.
    # This keeps future-looking values as NaN instead of inventing history.
    df["alarm_lag_1h"] = df.groupby("city")["alarm"].shift(1)
    df["alarm_lag_3h"] = df.groupby("city")["alarm"].shift(3)
    df["alarm_lag_6h"] = df.groupby("city")["alarm"].shift(6)
    df["alarm_lag_24h"] = df.groupby("city")["alarm"].shift(24)

    df["active_regions_count_lag1h"] = df.groupby("city")["active_regions_count"].shift(1)
    df["active_regions_count_lag3h"] = df.groupby("city")["active_regions_count"].shift(3)
    df["active_regions_count_lag6h"] = df.groupby("city")["active_regions_count"].shift(6)

    # Rolling features are also left as-is.
    # If the entire window is unknown, Pandas will keep the result as NaN.
    df["alarm_hours_last_24h"] = (
        df.groupby("city")["alarm"]
        .transform(lambda x: x.shift(1).rolling(24, min_periods=1).sum())
    )

    df["alarm_events_last_24h"] = (
        df.groupby("city")["events_in_hour"]
        .transform(lambda x: x.shift(1).rolling(24, min_periods=1).sum())
    )

    # 7. Keep only the forecast horizon and save it.
    df_forecast = df[(df["datetime"] >= forecast_start) & (df["datetime"] <= forecast_end)].copy()

    final_cols = [
        "datetime", "city",
        "alarm_lag_1h", "alarm_lag_3h", "alarm_lag_6h", "alarm_lag_24h",
        "alarm_hours_last_24h", "alarm_events_last_24h",
        "active_regions_count_lag1h", "active_regions_count_lag3h", "active_regions_count_lag6h"
    ]
    df_forecast = df_forecast[final_cols].sort_values(["city", "datetime"]).reset_index(drop=True)

    df_forecast.to_csv(OUTPUT_PATH, index=False)
    print(f"Success! Generated {len(df_forecast)} forecast rows. Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    process_alarms()
