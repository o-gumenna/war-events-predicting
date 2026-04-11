#!/usr/bin/env python3
"""
Flask REST API for serving alarm forecast predictions.

Endpoints:
  GET /api/forecast?region=Kyiv   — Get forecast for specific region
  GET /api/forecast?region=all    — Get forecast for all regions
  GET /health                      — Health check

Response format:
  {
    "last_model_train_time": "2026-04-10T16:50:00Z",
    "last_prediction_time": "2026-04-11T14:00:00Z",
    "regions_forecast": {
      "Kyiv": {
                "12:00": {"probability": 85, "alarm": true, "threats": {"ballistic": false, "drones": true, "cruise": false, "guided": false}},
                "13:00": {"probability": 80, "alarm": true, "threats": {"ballistic": true, "drones": false, "cruise": false, "guided": false}},
        ...
      },
      "Kharkiv": {...},
      ...
    }
  }

Usage:
  python app.py                  # Run on localhost:5000
  python app.py --host 0.0.0.0 --port 5000  # Public interface on port 5000
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

# ============================================================================
# CONFIGURATION
# ============================================================================

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PREDICTIONS_FILE = DATA_DIR / "predictions" / "predictions_latest.json"
LOGS_DIR = DATA_DIR / "logs"

# Create directories
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
log_file = LOGS_DIR / "api.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("api")

# Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configuration
app.config.update(
    JSON_SORT_KEYS=False,
    JSONIFY_PRETTYPRINT_REGULAR=True,
)

# ============================================================================
# CONSTANTS
# ============================================================================

ALL_CITIES = [
    'Cherkasy', 'Chernihiv', 'Chernivtsi', 'Dnipro', 'Donetsk', 'Ivano-Frankivsk',
    'Kharkiv', 'Kherson', 'Khmelnytskyi', 'Kropyvnytskyi', 'Kyiv', 'Lutsk',
    'Lviv', 'Mykolaiv', 'Odesa', 'Poltava', 'Rivne', 'Sumy', 'Ternopil',
    'Uzhhorod', 'Vinnytsia', 'Zaporizhzhia', 'Zhytomyr'
]

THREAT_LABELS = {
    "ballistic": "Балістичні ракети / МіГ-31",
    "drones": "Шахеди / Дрони",
    "cruise": "Крилаті ракети",
    "guided": "Керовані бомби",
}


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def load_predictions():
    """Load latest predictions from JSON file."""
    if not PREDICTIONS_FILE.exists():
        log.warning(f"Predictions file not found: {PREDICTIONS_FILE}")
        return None
    
    try:
        with open(PREDICTIONS_FILE, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        log.error(f"Failed to load predictions: {e}")
        return None


def filter_regions(data, region):
    """Filter predictions by region."""
    if region is None or region.lower() == "all":
        return data
    
    if region not in data.get("regions_forecast", {}):
        return None
    
    filtered_data = {
        "last_model_train_time": data.get("last_model_train_time"),
        "last_prediction_time": data.get("last_prediction_time"),
        "regions_forecast": {
            region: data["regions_forecast"][region]
        },
    }
    
    return filtered_data


# ============================================================================
# API ROUTES
# ============================================================================

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "alarm-forecast-api",
    }), 200


@app.route("/api/forecast", methods=["GET"])
def forecast():
    """
    Get forecast predictions.
    
    Query parameters:
      region (optional): specific region or 'all' (default: 'all')
    """
    try:
        region = request.args.get("region", "all")
        
        # Validate region if specified
        if region.lower() != "all" and region not in ALL_CITIES:
            return jsonify({
                "error": f"Invalid region: {region}",
                "available_regions": ALL_CITIES,
            }), 400
        
        # Load predictions
        data = load_predictions()
        if data is None:
            return jsonify({
                "error": "No predictions available. Please wait for batch prediction to complete.",
                "status": "predictions_not_ready",
            }), 503
        
        # Filter by region if requested
        if region.lower() != "all":
            data = filter_regions(data, region)
            if data is None:
                return jsonify({
                    "error": f"Region not found in predictions: {region}",
                }), 404
        
        log.info(f"✓ Forecast request for region: {region}")
        return jsonify(data), 200
        
    except Exception as e:
        log.error(f"✗ Forecast endpoint error: {e}", exc_info=True)
        return jsonify({
            "error": "Internal server error",
            "details": str(e),
        }), 500


@app.route("/api/regions", methods=["GET"])
def regions():
    """List all available regions."""
    data = load_predictions()
    available_regions = list(data.get("regions_forecast", {}).keys()) if data else ALL_CITIES
    
    return jsonify({
        "available_regions": available_regions,
        "total_regions": len(available_regions),
    }), 200


@app.route("/api/threat-types", methods=["GET"])
def threat_types():
    """Get multi-label threat definitions."""
    return jsonify({
        "threat_labels": THREAT_LABELS,
    }), 200


@app.route("/api/metadata", methods=["GET"])
def metadata():
    """Get API and model metadata."""
    data = load_predictions()
    
    return jsonify({
        "api_version": "1.0.0",
        "model_version": "v1_binary + v2_multilabel",
        "last_model_train_time": data.get("last_model_train_time") if data else None,
        "last_prediction_time": data.get("last_prediction_time") if data else None,
        "regions_count": len(ALL_CITIES),
        "forecast_hours": 24,
        "update_interval_seconds": 3600,
    }), 200


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({
        "error": "Endpoint not found",
        "path": request.path,
    }), 404


@app.errorhandler(500)
def server_error(error):
    """Handle 500 errors."""
    log.error(f"Server error: {error}", exc_info=True)
    return jsonify({
        "error": "Internal server error",
    }), 500


# ============================================================================
# CLI
# ============================================================================

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Alarm Forecast API")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    
    args = parser.parse_args()
    
    log.info("=" * 70)
    log.info(f"Starting Alarm Forecast API on {args.host}:{args.port}")
    log.info(f"Predictions file: {PREDICTIONS_FILE}")
    log.info("=" * 70)
    
    app.run(
        host=args.host,
        port=args.port,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
