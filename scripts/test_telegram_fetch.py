"""Small integration test for telegram_fetch pipeline.

This script:
- imports the existing `telegram_fetch` module
- sets temporary RAW_FILE and FEATURES_FILE paths
- builds a small synthetic raw-history DataFrame covering hours now-6..now
  for two cities
- injects non-zero counts at specific timestamps
- writes the raw CSV, runs `generate_features()` and checks that
  lag columns for T+1 contain expected values.

Run with:
python scripts/test_telegram_fetch.py

If assertions pass, prints OK.
"""

from pathlib import Path
import tempfile
from datetime import datetime, timedelta, timezone
import pandas as pd

import src.forecast_pipeline.telegram_fetch as tf
import os


def run_test():
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        # Redirect RAW_FILE and FEATURES_FILE to temp files
        raw_path = td_path / "telegram_raw_48h.csv"
        feat_path = td_path / "telegram_features_24h.csv"
        tf.RAW_FILE = raw_path
        tf.FEATURES_FILE = feat_path

        # Limit ALL_CITIES to a small set for test
        orig_cities = tf.ALL_CITIES
        test_cities = ['Kyiv', 'Kharkiv']
        tf.ALL_CITIES = test_cities

        try:
            # Build hours from now-6 .. now
            now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            hours = [now_utc - timedelta(hours=h) for h in range(6, -1, -1)]  # 7 rows: now-6..now

            # Threat count columns expected in RAW
            count_cols = [f'{th}_count' for th in tf.THREAT_DICT.keys()]

            rows = []
            for city in test_cities:
                for dt in hours:
                    row = {
                        'datetime': dt,
                        'city': city,
                    }
                    for c in count_cols:
                        row[c] = 0
                    rows.append(row)

            df_raw = pd.DataFrame(rows)

            # Inject some non-zero counts at specific times for Kyiv
            # Set tg_shaheds_count at now (lag1 source) = 3
            df_raw.loc[(df_raw['city'] == 'Kyiv') & (df_raw['datetime'] == now_utc), 'tg_shaheds_count'] = 3
            # Set tg_ballistic_count at now-2 (will be used for some lag positions)
            df_raw.loc[(df_raw['city'] == 'Kyiv') & (df_raw['datetime'] == now_utc - timedelta(hours=2)), 'tg_ballistic_count'] = 1
            # For Kharkiv set tg_cruise_count at now-1
            df_raw.loc[(df_raw['city'] == 'Kharkiv') & (df_raw['datetime'] == now_utc - timedelta(hours=1)), 'tg_cruise_count'] = 2

            # Save raw CSV
            df_raw.to_csv(raw_path, index=False)

            # Run generate_features (it will read tf.RAW_FILE and write tf.FEATURES_FILE)
            # Ensure Telethon absence won't break import paths (we don't call scraper here).
            tf.generate_features()

            # Load features and check values
            df_feat = pd.read_csv(feat_path)

            # Look for Kyiv row at T+1 (future hour = now+1)
            future_hour = now_utc + timedelta(hours=1)
            kyiv_row = df_feat[(df_feat['region'] == 'Kyiv') & (pd.to_datetime(df_feat['datetime']) == future_hour)]
            assert not kyiv_row.empty, "No Kyiv row for T+1 found in features"

            # Check tg_shaheds_count_lag1h == 3
            val = int(kyiv_row.iloc[0]['tg_shaheds_count_lag1h'])
            assert val == 3, f"Expected tg_shaheds_count_lag1h==3 for Kyiv T+1, got {val}"

            # Check tg_ballistic_count_lag3h for some future hour: for hours_ahead=3 it uses lag3_dt = now - (3-3)=now
            # but we set ballistic at now-2; check an appropriate target: for hours_ahead=1..3 mapping ensures
            # for hours_ahead=2, lag3_dt = now-1; for hours_ahead=3, lag3_dt = now
            # We set ballistic at now-2; it will appear as lag3 for hours_ahead where lag3_dt == now-2 → hours_ahead = 1? compute: lag3_dt = now - (3 - hours_ahead)
            # Solve for hours_ahead where now - (3 - h) == now-2 -> 3 - h = 2 -> h =1 => hours_ahead=1 so lag3 for T+1 should read now-2
            val_ballistic = int(kyiv_row.iloc[0]['tg_ballistic_count_lag3h'])
            assert val_ballistic == 1, f"Expected tg_ballistic_count_lag3h==1 for Kyiv T+1, got {val_ballistic}"

            # Kharkiv check: for T+1, tg_cruise_count_lag1h should be value at now (we set at now-1), so lag1 exists only for hours_ahead==1 mapping lag1_dt=now
            # That means our tg_cruise_count_lag1h for Kharkiv T+1 will be 0. But tg_cruise_count_lag3h for T+2 may pick now-1.
            kh_future_row = df_feat[(df_feat['region'] == 'Kharkiv') & (pd.to_datetime(df_feat['datetime']) == future_hour + timedelta(hours=1))]
            # For hours_ahead=2 (future_hour+1) lag3_dt = now - (3-2)=now-1 so should pick tg_cruise_count=2
            assert not kh_future_row.empty, "No Kharkiv row for T+2 found in features"
            val_cruise_lag3 = int(kh_future_row.iloc[0]['tg_cruise_count_lag3h'])
            assert val_cruise_lag3 == 2, f"Expected tg_cruise_count_lag3h==2 for Kharkiv T+2, got {val_cruise_lag3}"

            print("TEST OK: telegram_fetch aggregation and lag generation behave as expected")

        finally:
            # restore
            tf.ALL_CITIES = orig_cities


if __name__ == '__main__':
    run_test()
