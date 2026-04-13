#!/usr/bin/env python3
"""
Batch prediction script for hourly forecasting on AWS.

Runs every hour (via cron at :10 minute mark):
  - Reads final merged features (FINAL_FEATURES_24H.csv)
  - v1 (binary model)      -> alarm probability + alarm True/False per row
  - v2 (multi-label model) -> {ballistic, drones, cruise, guided} when alarm=True
  - Saves results to data/predictions/predictions_latest.json

Output format per city/hour:
  {
        "12:00": {"probability": 85, "alarm": true,  "threats": {"ballistic": false, "drones": true, "cruise": false, "guided": false}},
        "13:00": {"probability": 20, "alarm": false, "threats": null},
  }

Usage:
  python batch_predict.py
"""

import sys
import json
import logging
import pickle
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR     = PROJECT_ROOT / "data"

FEATURES_FILE    = DATA_DIR / "final" / "FINAL_FEATURES_24H.csv"
PREDICTIONS_DIR  = DATA_DIR / "predictions"
LOGS_DIR         = DATA_DIR / "logs"

# v1: binary model (alarm yes/no)
MODEL_v1_FILE = Path(__file__).parent / "3__hist_gradient_boosting__v1.pkl"
if not MODEL_v1_FILE.exists():
    MODEL_v1_FILE = PROJECT_ROOT / "models" / "3__hist_gradient_boosting__v1.pkl"

# v2: multi-label model (ballistic / drones / cruise / guided)
MODEL_v2_FILE = Path(__file__).parent / "3__hist_gradient_boosting__v2.pkl"
if not MODEL_v2_FILE.exists():
    MODEL_v2_FILE = PROJECT_ROOT / "models" / "3__hist_gradient_boosting__v2.pkl"

PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "batch_predict.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("batch_predict")


# ============================================================================
# HELPERS
# ============================================================================

def load_model_bundle(model_file):
    """Load pickle bundle."""
    try:
        with open(model_file, "rb") as f:
            bundle = pickle.load(f)
        log.info(f"  Loaded model from {model_file}")
        return bundle
    except Exception as e:
        log.error(f"  Failed to load {model_file}: {e}")
        raise


def load_features(features_file):
    if not features_file.exists():
        raise FileNotFoundError(f"Features file not found: {features_file}")
    df = pd.read_csv(features_file)
    log.info(f"  Loaded {len(df)} rows from {features_file}")
    return df


def prepare_X(df, expected_features):
    """Select and order expected feature columns."""
    df = df.copy()
    for col in expected_features:
        if col not in df.columns:
            log.warning(f"  Missing feature '{col}', filling with NaN")
            df[col] = np.nan
    # Keep a DataFrame with named columns to match model training input schema.
    X = df[expected_features].apply(pd.to_numeric, errors="coerce")
    return X


# ============================================================================
# PREDICTION
# ============================================================================

def run_batch_predictions(v1_bundle, v2_bundle, features_df):
    """
    Run both models.

    v1 bundle keys: model, threshold, features
    v2 bundle keys: model, thresholds (dict label->float), features, label_cols

    Returns: dict  city -> {HH:MM: {probability, alarm, threats}}
    """
    # v1: binary
    v1_model     = v1_bundle["model"]
    v1_threshold = v1_bundle["threshold"]
    v1_features  = v1_bundle["features"]

    X_v1     = prepare_X(features_df, v1_features)
    v1_probs = v1_model.predict_proba(X_v1)[:, 1]
    v1_alarm = v1_probs >= v1_threshold

    log.info(f"  v1 binary: {len(v1_probs)} rows, alarm rate {v1_alarm.mean()*100:.1f}%")

    # v2: multi-label (MultiOutputClassifier)
    v2_model      = v2_bundle["model"]
    v2_thresholds = v2_bundle["thresholds"]   # dict: label_name -> float
    v2_features   = v2_bundle["features"]
    v2_labels     = v2_bundle["label_cols"]   # ['label_ballistic', 'label_drones', 'label_cruise', 'label_guided']

    X_v2          = prepare_X(features_df, v2_features)
    # MultiOutputClassifier.predict_proba returns list of (n_rows, 2) arrays
    v2_proba_list = v2_model.predict_proba(X_v2)
    v2_proba      = np.column_stack([arr[:, 1] for arr in v2_proba_list])  # (n_rows, 4)

    v2_preds = np.column_stack([
        v2_proba[:, k] >= v2_thresholds[label]
        for k, label in enumerate(v2_labels)
    ])  # (n_rows, 4) bool

    for k, label in enumerate(v2_labels):
        log.info(f"  v2 {label}: {int(v2_preds[:, k].sum())} positive")

    label_idx = {label: k for k, label in enumerate(v2_labels)}

    # Assemble result dict
    if "region" in features_df.columns:
        regions = features_df["region"]
    elif "City" in features_df.columns:
        regions = features_df["City"]
    else:
        log.error("No 'region' or 'City' column in features")
        return {}

    predictions_by_city = {}
    city_hour_counter   = {}

    # Prefer real forecast timestamps as keys (HH:MM), fallback to sequential index
    datetimes = None
    if "datetime" in features_df.columns:
        datetimes = pd.to_datetime(features_df["datetime"], utc=True, errors="coerce")

    for idx in range(len(features_df)):
        city = regions.iloc[idx]

        if city not in predictions_by_city:
            predictions_by_city[city] = {}
            city_hour_counter[city]   = 0

        if datetimes is not None and not pd.isna(datetimes.iloc[idx]):
            hour_str = datetimes.iloc[idx].strftime("%H:%M")
            slot_datetime_utc = datetimes.iloc[idx].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            hour_str = str(city_hour_counter[city])
            city_hour_counter[city] += 1
            slot_datetime_utc = None

        prob  = float(v1_probs[idx])
        alarm = bool(v1_alarm[idx])

        if alarm:
            threats = {
                "ballistic": bool(v2_preds[idx, label_idx["label_ballistic"]]),
                "drones":    bool(v2_preds[idx, label_idx["label_drones"]]),
                "cruise":    bool(v2_preds[idx, label_idx["label_cruise"]]),
                "guided":    bool(v2_preds[idx, label_idx["label_guided"]]),
            }
        else:
            threats = None

        predictions_by_city[city][hour_str] = {
            "probability": int(round(prob * 100)),
            "alarm":       alarm,
            "threats":     threats,
            "slot_datetime_utc": slot_datetime_utc,
        }

    return predictions_by_city


# ============================================================================
# SAVE
# ============================================================================

def save_predictions(predictions_by_city):
    now_utc  = datetime.now(timezone.utc).isoformat()
    # Model trained on 2026-04-10 19:50 Kyiv (16:50 UTC) - not retraining automatically
    last_train_time = "2026-04-10T16:50:00Z"
    output   = {
        "last_model_train_time": last_train_time,
        "last_prediction_time":  now_utc,
        "regions_forecast":      predictions_by_city,
    }
    out_file = PREDICTIONS_DIR / "predictions_latest.json"
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)
    log.info(f"  Saved -> {out_file}")
    return out_file


# ============================================================================
# HEALTH CHECK + MAIN
# ============================================================================

def health_check():
    checks = [
        ("Features file", FEATURES_FILE),
        ("Model v1",      MODEL_v1_FILE),
        ("Model v2",      MODEL_v2_FILE),
    ]
    ok = True
    for name, path in checks:
        exists = path.exists()
        log.info(f"{'ok' if exists else 'MISSING'} {name}: {path}")
        if not exists:
            ok = False
    return ok


def main():
    log.info("=" * 70)
    log.info("BATCH PREDICTION - Hourly Forecast Generation")
    log.info("=" * 70)

    try:
        if not health_check():
            log.error("Health check failed — aborting")
            return 1

        log.info("\nLoading models...")
        v1_bundle = load_model_bundle(MODEL_v1_FILE)
        v2_bundle = load_model_bundle(MODEL_v2_FILE)

        log.info("\nLoading features...")
        features_df = load_features(FEATURES_FILE)

        log.info("\nGenerating predictions...")
        predictions = run_batch_predictions(v1_bundle, v2_bundle, features_df)

        if not predictions:
            log.error("No predictions generated")
            return 1

        log.info("\nSaving predictions...")
        save_predictions(predictions)

        log.info("=" * 70)
        log.info("BATCH PREDICTION COMPLETED SUCCESSFULLY")
        log.info("=" * 70)
        return 0

    except Exception as e:
        log.error(f"BATCH PREDICTION FAILED: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
