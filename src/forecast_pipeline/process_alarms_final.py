"""
alarm_forecast_features.py
────────────────────────────────────────────────────────────────────────────────
Generates alarm-derived features for the next 24 hours.

Logic:
  - Reads the raw alarm snapshot history from alarms_raw.json
  - For hour T+1: lag values reflect the actual state at hour T
  - For hours T+2…T+24: all lag values are 0 (no future information available)
  - alarm_hours_last_24h and alarm_events_last_24h are computed over the
    24 hours preceding each forecast hour (closed='left')

Output columns (subset required by the model):
  datetime, city,
  alarm_lag_1h, alarm_lag_3h, alarm_lag_6h, alarm_lag_24h,
  alarm_hours_last_24h, alarm_events_last_24h,
  active_regions_count_lag1h, active_regions_count_lag3h, active_regions_count_lag6h
────────────────────────────────────────────────────────────────────────────────
"""

import json
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

INPUT_PATH  = "data/alarms/alarms_raw.json"
OUTPUT_PATH = "data/alarms/alarms_features.csv"

ALL_REGIONS = [
    "Cherkasy", "Chernihiv", "Chernivtsi", "Dnipro", "Donetsk",
    "Ivano-Frankivsk", "Kharkiv", "Kherson", "Khmelnytskyi", "Kropyvnytskyi",
    "Kyiv", "Lutsk", "Lviv", "Mykolaiv", "Odesa", "Poltava", "Rivne",
    "Sumy", "Ternopil", "Uzhhorod", "Vinnytsia", "Zaporizhzhia", "Zhytomyr"
]


def load_raw_snapshots() -> pd.DataFrame:
    """
    Read alarms_raw.json and build a per-region hourly table.
    Each snapshot contains the set of regions with an active alarm at that moment.
    """
    path = Path(INPUT_PATH)
    if not path.exists():
        print(f"File not found: {INPUT_PATH}")
        return pd.DataFrame()

    with open(path, "r", encoding="utf-8") as f:
        records = json.load(f)

    rows = []
    for snapshot in records:
        collected_at   = pd.to_datetime(snapshot["collected_at"]).tz_convert("UTC")
        active_regions = set(snapshot.get("active_regions", []))
        for region in ALL_REGIONS:
            rows.append({
                "datetime": collected_at.floor("h"),
                "region":   region,
                "alarm":    1 if region in active_regions else 0,
            })

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["datetime", "region"])
    return df.sort_values(["region", "datetime"]).reset_index(drop=True)


def build_history_grid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a complete hourly grid from the earliest snapshot to the current hour T.
    Gaps (hours with no snapshot) are forward-filled within each region.
    """
    full_range = pd.date_range(
        start=df["datetime"].min(),
        end=df["datetime"].max(),
        freq="h",
        tz="UTC"
    )
    full_grid = pd.MultiIndex.from_product(
        [ALL_REGIONS, full_range], names=["region", "datetime"]
    )
    df_grid = pd.DataFrame(index=full_grid).reset_index()
    df_grid = df_grid.merge(df, on=["region", "datetime"], how="left")

    # Forward-fill missing hours within each region, then fill any leading NaN with 0
    df_grid["alarm"] = (
        df_grid.groupby("region")["alarm"]
        .transform(lambda x: x.ffill().fillna(0))
        .astype(int)
    )
    return df_grid.sort_values(["region", "datetime"]).reset_index(drop=True)


def compute_forecast_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute lag and rolling features for the 24-hour forecast window (T+1…T+24).

    For T+1 (the first forecast hour):
      alarm_lag_1h  = alarm state at T      (shift 1 from history)
      alarm_lag_3h  = alarm state at T-2    (shift 3 from history)
      alarm_lag_6h  = alarm state at T-5    (shift 6 from history)
      alarm_lag_24h = alarm state at T-23   (shift 24 from history)
      alarm_hours_last_24h   = sum of alarm over [T-24, T-1]
      alarm_events_last_24h  = number of alarm onsets over [T-24, T-1]
      active_regions_count_lag1h/3h/6h = active region count at T / T-2 / T-5

    For T+2…T+24: all lag values are 0 (no future alarm data available).
    """
    T = df["datetime"].max()

    # active_regions_count per hour (number of regions with alarm == 1)
    active_per_hour = (
        df.groupby("datetime")["alarm"].sum()
        .reset_index()
        .rename(columns={"alarm": "active_regions_count"})
    )
    df = df.merge(active_per_hour, on="datetime", how="left")

    # alarm onset flag (1 when alarm starts: current=1 and previous=0)
    df["alarm_start"] = (
        df.groupby("region")["alarm"]
        .transform(lambda x: ((x == 1) & (x.shift(1).fillna(0) == 0)).astype(int))
    )

    # rolling features over the 24h window preceding each hour (closed='left')
    df = df.sort_values(["region", "datetime"]).reset_index(drop=True)
    df.index = pd.DatetimeIndex(df["datetime"])

    df["alarm_hours_last_24h"] = (
        df.groupby("region")["alarm"]
        .transform(lambda x: x.rolling("24h", closed="left").sum())
    ).values

    df["alarm_events_last_24h"] = (
        df.groupby("region")["alarm_start"]
        .transform(lambda x: x.rolling("24h", closed="left").sum())
    ).values

    df = df.reset_index(drop=True)
    df["alarm_hours_last_24h"]  = df["alarm_hours_last_24h"].fillna(0).astype(int)
    df["alarm_events_last_24h"] = df["alarm_events_last_24h"].fillna(0).astype(int)

    # lag features computed from the historical series
    df["alarm_lag_1h"]  = df.groupby("region")["alarm"].shift(1).fillna(0).astype(int)
    df["alarm_lag_3h"]  = df.groupby("region")["alarm"].shift(3).fillna(0).astype(int)
    df["alarm_lag_6h"]  = df.groupby("region")["alarm"].shift(6).fillna(0).astype(int)
    df["alarm_lag_24h"] = df.groupby("region")["alarm"].shift(24).fillna(0).astype(int)

    df["active_regions_count_lag1h"] = (
        df.groupby("region")["active_regions_count"].shift(1).fillna(0).astype(int)
    )
    df["active_regions_count_lag3h"] = (
        df.groupby("region")["active_regions_count"].shift(3).fillna(0).astype(int)
    )
    df["active_regions_count_lag6h"] = (
        df.groupby("region")["active_regions_count"].shift(6).fillna(0).astype(int)
    )

    # Extract only the row at hour T — this is the feature state for T+1
    state_at_T = df[df["datetime"] == T].copy()

    feature_cols = [
        "region",
        "alarm_lag_1h", "alarm_lag_3h", "alarm_lag_6h", "alarm_lag_24h",
        "alarm_hours_last_24h", "alarm_events_last_24h",
        "active_regions_count_lag1h", "active_regions_count_lag3h", "active_regions_count_lag6h",
    ]
    state_at_T = state_at_T[feature_cols].copy()

    # Build 24 forecast rows
    # T+1 carries the real lag values from T; T+2…T+24 have all lags = 0
    lag_cols = [c for c in feature_cols if c != "region"]
    rows = []

    for h in range(1, 25):
        future_time = T + pd.Timedelta(hours=h)
        for _, region_state in state_at_T.iterrows():
            row = {"datetime": future_time, "city": region_state["region"]}
            if h == 1:
                # carry forward the last known lag values
                for col in lag_cols:
                    row[col] = region_state[col]
            else:
                # no information available for future hours
                for col in lag_cols:
                    row[col] = 0
            rows.append(row)

    df_forecast = pd.DataFrame(rows)

    final_cols = [
        "datetime", "city",
        "alarm_lag_1h", "alarm_lag_3h", "alarm_lag_6h", "alarm_lag_24h",
        "alarm_hours_last_24h", "alarm_events_last_24h",
        "active_regions_count_lag1h", "active_regions_count_lag3h", "active_regions_count_lag6h",
    ]
    return df_forecast[final_cols].sort_values(["city", "datetime"]).reset_index(drop=True)


def process_alarms():
    print(f"Processing alarms at {datetime.now(timezone.utc).isoformat()}...")

    df = load_raw_snapshots()
    if df.empty:
        print("No data to process.")
        return

    print(f"History up to: {df['datetime'].max()}")

    df = build_history_grid(df)
    df_forecast = compute_forecast_features(df)

    print(f"Generated {len(df_forecast)} forecast rows.")

    path = Path(OUTPUT_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    df_forecast.to_csv(path, index=False)
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    process_alarms()