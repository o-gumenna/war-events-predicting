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
    now_utc = datetime.now(timezone.utc)
    print(f"[{now_utc.isoformat()}] Processing hourly features...")

    path = Path(INPUT_PATH)
    if not path.exists():
        print(f"No raw history found at {INPUT_PATH}. Wait for collector to run.")
        return

    # 1. Завантажуємо 5-хвилинні факти
    df_raw = pd.read_csv(path)
    df_raw["datetime"] = pd.to_datetime(df_raw["datetime"], utc=True)
    df_raw = df_raw.sort_values(["city", "datetime"]).reset_index(drop=True)

    # 2. РАХУЄМО ПОДІЇ (Включення сирен) НА 5-ХВИЛИННОМУ РІВНІ
    df_raw["alarm_start_5min"] = df_raw.groupby("city")["alarm"].transform(
        lambda x: ((x == 1) & (x.shift(1).fillna(0) == 0)).astype(int)
    )

    # 3. АГРЕГАЦІЯ У ГОДИННІ ВІКНА
    df_raw["datetime_h"] = df_raw["datetime"].dt.floor("h")

    # А) Факт тривоги в годині (якщо була хоч раз -> 1)
    df_hourly_alarm = df_raw.groupby(["city", "datetime_h"])["alarm"].max().reset_index()

    # Б) Скільки разів вмикалась сирена за цю годину (сумуємо старти)
    df_hourly_events = df_raw.groupby(["city", "datetime_h"])["alarm_start_5min"].sum().reset_index()
    df_hourly_events.rename(columns={"alarm_start_5min": "events_in_hour"}, inplace=True)

    # Об'єднуємо факт та події
    df = df_hourly_alarm.merge(df_hourly_events, on=["city", "datetime_h"])
    df.rename(columns={"datetime_h": "datetime"}, inplace=True)

    # 4. ВИЗНАЧАЄМО МЕЖІ ЧАСУ
    # Поточний час Т (остання повністю закрита година)
    T = now_utc.floor("h") - timedelta(hours=1)

    forecast_start = T + timedelta(hours=1)
    forecast_end = T + timedelta(hours=24)

    # 5. БУДУЄМО СІТКУ (Історія + 24 години майбутнього)
    full_range = pd.date_range(start=df["datetime"].min(), end=forecast_end, freq="h", tz="UTC")
    full_grid = pd.MultiIndex.from_product([ALL_REGIONS, full_range], names=["city", "datetime"])
    df_grid = pd.DataFrame(index=full_grid).reset_index()

    df = df_grid.merge(df, on=["city", "datetime"], how="left")

    # Майбутнє заповнюється нулями
    df["alarm"] = df["alarm"].fillna(0).astype(int)
    df["events_in_hour"] = df["events_in_hour"].fillna(0).astype(int)

    # Рахуємо активні регіони по всій Україні
    active_per_hour = df.groupby("datetime")["alarm"].sum().reset_index()
    active_per_hour.rename(columns={"alarm": "active_regions_count"}, inplace=True)
    df = df.merge(active_per_hour, on="datetime", how="left")
    df = df.sort_values(["city", "datetime"]).reset_index(drop=True)

    # 6. РАХУЄМО ЛАГИ І РОЛЛІНГИ (Тепер вони бездоганно спадають у майбутнє)
    df["alarm_lag_1h"] = df.groupby("city")["alarm"].shift(1).fillna(0).astype(int)
    df["alarm_lag_3h"] = df.groupby("city")["alarm"].shift(3).fillna(0).astype(int)
    df["alarm_lag_6h"] = df.groupby("city")["alarm"].shift(6).fillna(0).astype(int)
    df["alarm_lag_24h"] = df.groupby("city")["alarm"].shift(24).fillna(0).astype(int)

    df["active_regions_count_lag1h"] = df.groupby("city")["active_regions_count"].shift(1).fillna(0).astype(int)
    df["active_regions_count_lag3h"] = df.groupby("city")["active_regions_count"].shift(3).fillna(0).astype(int)
    df["active_regions_count_lag6h"] = df.groupby("city")["active_regions_count"].shift(6).fillna(0).astype(int)

    df.index = pd.DatetimeIndex(df["datetime"])

    # Сума годин з тривогами за останні 24г
    df["alarm_hours_last_24h"] = (
        df.groupby("city")["alarm"]
        .transform(lambda x: x.rolling("24h", closed="left").sum())
    ).values

    # Сума запусків сирен за останні 24г
    df["alarm_events_last_24h"] = (
        df.groupby("city")["events_in_hour"]
        .transform(lambda x: x.rolling("24h", closed="left").sum())
    ).values

    df = df.reset_index(drop=True)
    df["alarm_hours_last_24h"] = df["alarm_hours_last_24h"].fillna(0).astype(int)
    df["alarm_events_last_24h"] = df["alarm_events_last_24h"].fillna(0).astype(int)

    # 7. ВІДРІЗАЄМО МАЙБУТНЄ І ЗБЕРІГАЄМО
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