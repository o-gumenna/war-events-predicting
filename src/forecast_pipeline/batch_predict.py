#!/usr/bin/env python3
"""
Batch prediction script for hourly forecasting on AWS.

Runs every hour (via cron at :08 minute mark):
  - Reads final merged features (FINAL_FEATURES_24H.csv)
  - Loads binary model (v1)
  - Generates predictions for all 23 regions × 24 hours
  - Saves results to data/predictions/predictions_latest.json
  - Logs execution to data/logs/batch_predict.log

Usage:
  python batch_predict.py
"""

import os
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

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
FEATURES_FILE = DATA_DIR / "final" / "FINAL_FEATURES_24H.csv"
PREDICTIONS_DIR = DATA_DIR / "predictions"
LOGS_DIR = DATA_DIR / "logs"

# Model files (should be in same folder as this script or in project root)
MODEL_v1_FILE = Path(__file__).parent / "3__hist_gradient_boosting__v1.pkl"
if not MODEL_v1_FILE.exists():
    MODEL_v1_FILE = PROJECT_ROOT / "models" / "3__hist_gradient_boosting__v1.pkl"

# Create directories if missing
PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Logging
log_file = LOGS_DIR / "batch_predict.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("batch_predict")

# ============================================================================
# CONSTANTS
# ============================================================================

ALL_CITIES = [
    'Cherkasy', 'Chernihiv', 'Chernivtsi', 'Dnipro', 'Donetsk', 'Ivano-Frankivsk',
    'Kharkiv', 'Kherson', 'Khmelnytskyi', 'Kropyvnytskyi', 'Kyiv', 'Lutsk',
    'Lviv', 'Mykolaiv', 'Odesa', 'Poltava', 'Rivne', 'Sumy', 'Ternopil',
    'Uzhhorod', 'Vinnytsia', 'Zaporizhzhia', 'Zhytomyr'
]

THREAT_TYPES = {
    0: "Немає загрози",
    1: "Балістична",
    2: "Дрони (Шахеди)",
    3: "Інша загроза",
    4: "Комбінована атака"
}


# ============================================================================
# BATCH PREDICTION
# ============================================================================

def load_model_bundle(model_file):
    """Load model bundle (model + threshold + features list)."""
    try:
        with open(model_file, "rb") as f:
            bundle = pickle.load(f)
        
        log.info(f"✓ Loaded model from {model_file}")
        return {
            "model": bundle.get("model"),
            "threshold": bundle.get("threshold", 0.5),
            "features": bundle.get("features", []),
            "label_cols": bundle.get("label_cols", []),
        }
    except Exception as e:
        log.error(f"✗ Failed to load model: {e}")
        raise


def load_features(features_file):
    """Load merged features CSV."""
    if not features_file.exists():
        log.error(f"✗ Features file not found: {features_file}")
        raise FileNotFoundError(f"Features file not found: {features_file}")
    
    df = pd.read_csv(features_file)
    log.info(f"✓ Loaded {len(df)} rows from {features_file}")
    
    return df


def prepare_features_for_model(df, expected_features):
    """
    Prepare feature matrix for model.
    
    Ensures columns match expected features (handles missing with NaN fillna).
    Drop 'region' and 'datetime' columns (not used by model).
    """
    # Ensure all expected features are present
    for col in expected_features:
        if col not in df.columns:
            log.warning(f"  Missing feature: {col}, filling with NaN")
            df[col] = np.nan
    
    # Select only expected features in correct order
    X = df[expected_features].copy()
    
    # Fill any remaining NaN with median or 0
    X = X.fillna(X.median(numeric_only=True)).fillna(0)
    
    return X


def run_batch_predictions(model_bundle, features_df):
    """
    Run batch predictions.
    
    Returns:
      dict: {city: {hour: {probability, threat_type}}}
    """
    model = model_bundle["model"]
    threshold = model_bundle["threshold"]
    expected_features = model_bundle["features"]
    
    # Prepare feature matrix
    X = prepare_features_for_model(features_df, expected_features)
    
    # Get probabilities (positive class)
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(X)[:, 1]  # P(alarm=1)
    else:
        # Fallback for models without predict_proba
        probabilities = model.predict(X)
    
    # Make predictions
    predictions = (probabilities >= threshold).astype(int)
    
    log.info(f"✓ Generated {len(predictions)} predictions")
    log.info(f"  Alarm rate: {predictions.sum() / len(predictions) * 100:.1f}%")
    
    # Organize by city and hour
    predictions_by_city = {}
    
    regions = features_df.get("region", features_df.get("City", None))
    datetimes = features_df.get("datetime", None)
    
    if regions is None:
        log.error("✗ No 'region' column in features")
        return {}
    
    for idx, (city, dt, prob, pred) in enumerate(
        zip(regions, datetimes if datetimes is not None else [""] * len(regions), 
            probabilities, predictions)
    ):
        if city not in predictions_by_city:
            predictions_by_city[city] = {}
        
        # Extract hour from datetime or use index
        if isinstance(dt, str):
            try:
                hour = pd.to_datetime(dt).hour
            except:
                hour = idx % 24
        else:
            hour = idx % 24
        
        predictions_by_city[city][hour] = {
            "probability": int(prob * 100),  # Convert to 0-100 scale
            "threat_type": int(pred),        # 0 or 1 for binary model
            "confidence": float(max(prob, 1 - prob)),  # Confidence in prediction
        }
    
    return predictions_by_city


def save_predictions(predictions_by_city):
    """Save predictions to JSON."""
    now_utc = datetime.now(timezone.utc).isoformat() + "Z"
    
    # Get model training time (from pickle metadata or use default)
    model_train_time = now_utc  # TODO: store this in model bundle
    
    output = {
        "last_model_train_time": model_train_time,
        "last_prediction_time": now_utc,
        "regions_forecast": predictions_by_city,
    }
    
    output_file = PREDICTIONS_DIR / "predictions_latest.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    
    log.info(f"✓ Saved predictions to {output_file}")
    return output_file


def health_check():
    """Verify all required files exist."""
    checks = [
        ("Features file", FEATURES_FILE, FEATURES_FILE.exists()),
        ("Model (v1)", MODEL_v1_FILE, MODEL_v1_FILE.exists()),
        ("Logs directory", LOGS_DIR, LOGS_DIR.exists()),
        ("Predictions directory", PREDICTIONS_DIR, PREDICTIONS_DIR.exists()),
    ]
    
    all_ok = True
    for name, path, ok in checks:
        status = "✓" if ok else "✗"
        log.info(f"{status} {name}: {path}")
        if not ok:
            all_ok = False
    
    return all_ok


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point."""
    log.info("=" * 70)
    log.info("BATCH PREDICTION - Hourly Forecast Generation")
    log.info("=" * 70)
    
    try:
        # Health check
        if not health_check():
            log.error("✗ Health check failed")
            return 1
        
        # Load model
        log.info("\nLoading model...")
        model_bundle = load_model_bundle(MODEL_v1_FILE)
        
        # Load features
        log.info("\nLoading features...")
        features_df = load_features(FEATURES_FILE)
        
        # Run predictions
        log.info("\nGenerating predictions...")
        predictions = run_batch_predictions(model_bundle, features_df)
        
        if not predictions:
            log.error("✗ No predictions generated")
            return 1
        
        # Save predictions
        log.info("\nSaving predictions...")
        save_predictions(predictions)
        
        log.info("=" * 70)
        log.info("✓ BATCH PREDICTION COMPLETED SUCCESSFULLY")
        log.info("=" * 70)
        return 0
        
    except Exception as e:
        log.error(f"✗ BATCH PREDICTION FAILED: {e}", exc_info=True)
        log.error("=" * 70)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
