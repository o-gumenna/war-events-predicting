# WarEvents

WarEvents is a forecasting project that combines data collection, feature engineering, batch prediction, a small Flask API, and a React frontend.

The project is organized as one repository with one backend API. Data collection and forecasting scripts prepare files in `data/`, `src/forecast_pipeline/batch_predict.py` generates the latest prediction JSON, `src/saas/app.py` serves it through REST endpoints, and `alarm_pred/` displays the forecast in the frontend. Project runs on an AWS Ubuntu server and is updated by cron jobs.

This repository operates as three connected parts:

- `alarm_pred/` contains the frontend
- `src/saas/` contains the backend API
- `src/forecast_pipeline/` contains the ML and prediction pipeline

Repository structure:

- `alarm_pred/` React + Vite frontend
- `src/saas/` Flask API
- `src/forecast_pipeline/` feature preparation and batch prediction
- `src/data_collection/` older and auxiliary data collection scripts
- `notebooks/` research and training notebooks
- `docs/` project documentation

How to install:

```bash
pip install -r requirements.txt
cd alarm_pred
npm install
cd ..
```

Backend environment:

Create a root `.env` file based on `.env.example` if you want to run data collection scripts or use external APIs.

Frontend environment:

The frontend can work in two modes:

- same-origin mode, where it requests `/api/forecast`
- direct API mode, where `VITE_API_BASE_URL` points to the backend

Use `alarm_pred/.env.example` as a reference for frontend variables.

How to run the backend API:

```bash
python src/saas/app.py --host 127.0.0.1 --port 5000
```

Main endpoints:

- `GET /health`
- `GET /api/forecast?region=all`
- `GET /api/forecast?region=Kyiv`
- `GET /api/regions`
- `GET /api/metadata`

How to run the frontend locally:

```bash
cd alarm_pred
npm run dev
```

By default, Vite proxies `/api` requests to `http://127.0.0.1:5000`.

Core pipeline commands:

```bash
python src/forecast_pipeline/isw_fetch.py
python src/forecast_pipeline/get_alarms.py
python src/forecast_pipeline/telegram_fetch.py
python src/forecast_pipeline/weather_forecast.py
python src/forecast_pipeline/process_alarms_final.py
python src/forecast_pipeline/merge_features.py
python src/forecast_pipeline/batch_predict.py
```

Testing:

```bash
pytest -q
```

Additional notes:

- `src/data_collection/` and `notebooks/` are useful for research and historical preparation, but they are not required for understanding the main frontend + API flow.
- the repository contains forecasting output, not an official warning system
- if you want deployment details, see `docs/deployment.md`
- if you want a short architecture overview, see `docs/pipeline.md`
