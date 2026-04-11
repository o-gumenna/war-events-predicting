# War Events Prediction SaaS

Python project for hourly air-alert forecasting by region in Ukraine.

## Unified Architecture

This repository should run as a single project with one canonical backend API:

1. Data collectors and feature scripts write CSV files into `data/`
2. `src/forecast_pipeline/merge_features.py` builds `data/final/FINAL_FEATURES_24H.csv`
3. `src/forecast_pipeline/batch_predict.py` generates `data/predictions/predictions_latest.json`
4. `src/saas/app.py` serves that JSON via REST (`/api/forecast`)
5. `alarm_pred/` frontend reads `/api/forecast`

Important: `src/saas/app.py` is the main API for this project.
There is no separate Flask backend inside `alarm_pred/`.

## Repository Structure

- `src/data_collection/` data collection scripts
- `src/forecast_pipeline/` feature engineering + prediction batch
- `src/saas/` Flask API for frontend integration
- `alarm_pred/` Vite + React frontend

## Installation

1. Clone repository

```bash
git clone <your-repo-url>
cd <your-repo-name>
```

2. Install Python dependencies

```bash
pip install -r requirements.txt
```

3. Install frontend dependencies

```bash
cd alarm_pred
npm install
cd ..
```

## Frontend-Backend Integration

Frontend supports two modes:

- Same-origin mode (default): frontend requests `/api/forecast`
- Direct API mode: set `VITE_API_BASE_URL` to call backend directly

For local dev proxy, configure target API via env:

```bash
cd alarm_pred
export VITE_API_PROXY_TARGET=http://127.0.0.1:5000
npm run dev
```

Optional direct mode (without proxy path rewriting in code):

```bash
cd alarm_pred
export VITE_API_BASE_URL=http://127.0.0.1:5000
npm run dev
```

For production on server:

1. Build frontend in `alarm_pred/` using `npm run build`
2. Serve static files from `alarm_pred/dist/` via Nginx (or another static server)
3. Route `/api/*` to Flask API from `src/saas/app.py`

In this setup, frontend does not require a separate backend process.

## Environment Variables

Server root `.env` remains backend-only (API credentials and collectors keys).
Frontend env is separate and optional in `alarm_pred/.env` (Vite build-time vars).

- `VITE_API_BASE_URL`: optional direct API URL for browser requests
- `VITE_API_PROXY_TARGET`: dev-only proxy target for `npm run dev`

## Run API

```bash
python src/saas/app.py --host 0.0.0.0 --port 5000
```

Main endpoint:

- `GET /api/forecast?region=all`
- `GET /api/forecast?region=Kyiv`

## Core Pipeline Commands

```bash
python src/forecast_pipeline/isw_fetch.py
python src/forecast_pipeline/get_alarms.py
python src/forecast_pipeline/telegram_fetch.py
python src/forecast_pipeline/weather_forecast.py
python src/forecast_pipeline/process_alarms_final.py
python src/forecast_pipeline/merge_features.py
python src/forecast_pipeline/batch_predict.py
```