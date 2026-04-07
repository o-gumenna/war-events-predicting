import pandas as pd
import json
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
                "alarm_actual": 1 if region in active_regions else 0,
            })

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["datetime", "region"])
    return df.sort_values(["region", "datetime"]).reset_index(drop=True)


def process_alarms():
    print(f"Processing alarms at {datetime.now(timezone.utc).isoformat()}...")

    df = load_raw_snapshots()
    if df.empty:
        print("No data to process.")
        return

    # 1. Визначаємо межі: зараз (Т) та 24 години майбутнього
    T = df["datetime"].max()
    forecast_start = T + pd.Timedelta(hours=1)
    forecast_end = T + pd.Timedelta(hours=24)

    # 2. Будуємо сітку від початку історії до кінця ПРОГНОЗУ (Т+24)
    full_range = pd.date_range(start=df["datetime"].min(), end=forecast_end, freq="h", tz="UTC")
    full_grid = pd.MultiIndex.from_product([ALL_REGIONS, full_range], names=["region", "datetime"])
    df_grid = pd.DataFrame(index=full_grid).reset_index()

    # 3. Накладаємо історію. Майбутні години отримають NaN -> заповнюємо 0 (невідоме майбутнє)
    df = df_grid.merge(df, on=["region", "datetime"], how="left")
    df["alarm_actual"] = df["alarm_actual"].fillna(0).astype(int)

    # 4. Рахуємо active_regions_count (сума по всіх областях)
    active_per_hour = df.groupby("datetime")["alarm_actual"].sum().reset_index()
    active_per_hour.rename(columns={"alarm_actual": "active_regions_count"}, inplace=True)
    df = df.merge(active_per_hour, on="datetime", how="left")

    df = df.sort_values(["region", "datetime"]).reset_index(drop=True)

    # 5. Рахуємо ЛАГИ. Pandas сам візьме реальні дані для минулого і підставлені нулі для майбутнього
    df["alarm_lag_1h"]  = df.groupby("region")["alarm_actual"].shift(1).fillna(0).astype(int)
    df["alarm_lag_3h"]  = df.groupby("region")["alarm_actual"].shift(3).fillna(0).astype(int)
    df["alarm_lag_6h"]  = df.groupby("region")["alarm_actual"].shift(6).fillna(0).astype(int)
    df["alarm_lag_24h"] = df.groupby("region")["alarm_actual"].shift(24).fillna(0).astype(int)

    df["active_regions_count_lag1h"] = df.groupby("region")["active_regions_count"].shift(1).fillna(0).astype(int)
    df["active_regions_count_lag3h"] = df.groupby("region")["active_regions_count"].shift(3).fillna(0).astype(int)
    df["active_regions_count_lag6h"] = df.groupby("region")["active_regions_count"].shift(6).fillna(0).astype(int)

    # 6. Рахуємо ROLLING за попередні 24 години (closed='left' ідеально підходить)
    df.index = pd.DatetimeIndex(df["datetime"])

    df["alarm_hours_last_24h"] = (
        df.groupby("region")["alarm_actual"]
        .transform(lambda x: x.rolling("24h", closed="left").sum())
    ).values

    df["alarm_start"] = (
        df.groupby("region")["alarm_actual"]
        .transform(lambda x: ((x == 1) & (x.shift(1).fillna(0) == 0)).astype(int))
    )

    df["alarm_events_last_24h"] = (
        df.groupby("region")["alarm_start"]
        .transform(lambda x: x.rolling("24h", closed="left").sum())
    ).values

    df = df.reset_index(drop=True)
    df["alarm_hours_last_24h"] = df["alarm_hours_last_24h"].fillna(0).astype(int)
    df["alarm_events_last_24h"] = df["alarm_events_last_24h"].fillna(0).astype(int)

    # 7. Відрізаємо ТІЛЬКИ МАЙБУТНЄ (Т+1 ... Т+24)
    df_forecast = df[(df["datetime"] >= forecast_start) & (df["datetime"] <= forecast_end)].copy()

    # 8. Залишаємо виключно потрібні моделі колонки
    df_forecast = df_forecast.rename(columns={"region": "city"})
    final_cols = [
        "datetime", "city",
        "alarm_lag_1h", "alarm_lag_3h", "alarm_lag_6h", "alarm_lag_24h",
        "alarm_hours_last_24h", "alarm_events_last_24h",
        "active_regions_count_lag1h", "active_regions_count_lag3h", "active_regions_count_lag6h"
    ]

    df_forecast = df_forecast[final_cols].sort_values(["city", "datetime"]).reset_index(drop=True)

    # Зберігаємо
    path = Path(OUTPUT_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    df_forecast.to_csv(path, index=False)
    print(f"Generated {len(df_forecast)} forecast rows. Saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    process_alarms()