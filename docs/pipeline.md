Pipeline overview

The repository has three main layers.

Frontend:

- `alarm_pred/main.jsx`
- `alarm_pred/ukraine-alerts.jsx`
- `alarm_pred/vite.config.js`
- `alarm_pred/public/Ukraine-regions.json`

The frontend renders the UI and requests forecast data from `/api/forecast`.

Backend API:

- `src/saas/app.py`

The API reads `data/predictions/predictions_latest.json` and exposes it through REST endpoints used by the frontend.

Prediction pipeline:

- `src/forecast_pipeline/get_alarms.py`
- `src/forecast_pipeline/isw_fetch.py`
- `src/forecast_pipeline/telegram_fetch.py`
- `src/forecast_pipeline/weather_forecast.py`
- `src/forecast_pipeline/process_alarms_final.py`
- `src/forecast_pipeline/merge_features.py`
- `src/forecast_pipeline/batch_predict.py`

These scripts prepare features, assemble the final dataset, and generate the latest forecast JSON.

Supporting and research material:

- `src/data_collection/` contains older, helper, and historical collection scripts
- `notebooks/` contains exploration, model training, and analysis notebooks
- `tests/` contains automated tests for the main pipeline scripts

Main runtime flow:

1. source data is collected or refreshed
2. features are merged into `data/final/FINAL_FEATURES_24H.csv`
3. predictions are generated into `data/predictions/predictions_latest.json`
4. the Flask API serves this JSON
5. the frontend displays the forecast

Important project boundaries:

- `alarm_pred/` is frontend only
- `src/saas/` is the backend API
- `src/forecast_pipeline/` is the active prediction pipeline
- `src/data_collection/` is not the main API/backend layer

If someone wants to build on this repository, the safest starting point is:

1. understand the API in `src/saas/app.py`
2. understand the frontend request flow in `alarm_pred/ukraine-alerts.jsx`
3. understand how `src/forecast_pipeline/batch_predict.py` creates the response file used by the API
