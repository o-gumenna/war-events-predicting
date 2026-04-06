import pandas as pd

def build_master_dataset(weather_file, alarm_file, reddit_file, isw_file, telegram_file, output_file):
    print("Loading and standardizing data...")

    # base: weather (most granular dataset)
    df_weather = pd.read_csv(weather_file)
    df_weather['datetime'] = pd.to_datetime(df_weather['datetime'], utc=True)
    print(f"Weather loaded: {len(df_weather):,} rows")

    # air alarms
    df_alarms = pd.read_csv(alarm_file)
    df_alarms = df_alarms.rename(columns={'region': 'city'})
    df_alarms['datetime'] = pd.to_datetime(df_alarms['datetime'], utc=True)

    # reddit (national-level hourly aggregates)
    df_reddit = pd.read_csv(reddit_file)
    df_reddit = df_reddit.rename(columns={'hour_slot': 'datetime'})
    df_reddit['datetime'] = pd.to_datetime(df_reddit['datetime'], utc=True)

    # ISW reports — денна гранулярність з точним часом публікації
    df_isw = pd.read_csv(isw_file)
    df_isw['datetime'] = pd.to_datetime(df_isw['datetime'], utc=True)
    df_isw = df_isw.drop(columns=['date'], errors='ignore')
    df_isw = df_isw.sort_values('datetime').reset_index(drop=True)

    # Telegram (city-level hourly aggregates)
    df_telegram = pd.read_csv(telegram_file)
    df_telegram = df_telegram.rename(columns={'hour_slot': 'datetime'})
    df_telegram['datetime'] = pd.to_datetime(df_telegram['datetime'], utc=True)

    # Step 1: weather + alarms
    print("Step 1: merging weather and alarms...")
    df_master = pd.merge(df_weather, df_alarms, on=['city', 'datetime'], how='left')
    alarm_cols = [c for c in df_alarms.columns if c not in ['city', 'datetime']]
    df_master[alarm_cols] = df_master[alarm_cols].fillna(0).astype(int)
    print(f"After alarms merge: {len(df_master):,} rows")

    # Step 2: Reddit (national-level, no city key)
    print("Step 2: adding Reddit features...")
    df_master = pd.merge(df_master, df_reddit, on='datetime', how='left')
    reddit_cols = df_reddit.columns.difference(['datetime'])
    df_master[reddit_cols] = df_master[reddit_cols].fillna(0)
    print(f"After Reddit merge: {len(df_master):,} rows")

    # Step 3: Telegram (city-level)
    print("Step 3: adding Telegram features...")
    df_master = pd.merge(df_master, df_telegram, on=['city', 'datetime'], how='left')
    telegram_cols = df_telegram.columns.difference(['city', 'datetime'])
    df_master[telegram_cols] = df_master[telegram_cols].fillna(0)
    print(f"After Telegram merge: {len(df_master):,} rows")

    # Step 4: ISW — merge_asof backward
    print("Step 4: merging ISW reports (merge_asof backward)...")
    df_master = df_master.sort_values('datetime').reset_index(drop=True)
    isw_cols = [c for c in df_isw.columns if c != 'datetime']
    df_master = pd.merge_asof(
        df_master,
        df_isw,
        on='datetime',
        direction='backward'
    )
    df_master[isw_cols] = df_master[isw_cols].fillna(0)
    print(f"After ISW merge: {len(df_master):,} rows")

    print(f"\n=== Final validation ===")
    print(f"Shape: {df_master.shape}")
    print(f"NaN remaining: {df_master.isna().sum().sum()}")
    print(f"Alarm rate: {df_master['alarm'].mean()*100:.1f}%")
    print(f"Date range: {df_master['datetime'].min()} → {df_master['datetime'].max()}")
    print(f"Unique cities: {df_master['city'].nunique()}")

    print(f"\nSaving to {output_file}...")
    df_master.to_csv(output_file, index=False)
    print(f"Done! Final shape: {len(df_master):,} rows × {len(df_master.columns)} columns")

    return df_master


if __name__ == "__main__":
    weather_csv  = 'weather_features_hourly.csv'
    alarm_csv    = 'alarms_hourly.csv'
    reddit_csv   = 'reddit_features_hourly.csv'
    isw_csv      = 'isw_reports_features.csv'
    telegram_csv = 'telegram_features_hourly.csv'
    output_csv   = 'merged_dataset_training.csv'

    final_dataset = build_master_dataset(
        weather_csv, alarm_csv, reddit_csv, isw_csv, telegram_csv, output_csv
    )