Deployment overview

This project can be deployed as a small server application with two runtime parts:

- a Flask API from `src/saas/app.py`
- a static frontend build from `alarm_pred/`

The prediction file is generated separately by the batch pipeline and then served by the API.

Expected deployment flow:

1. install Python dependencies
2. install frontend dependencies
3. build the frontend
4. make sure model files and generated data files are available
5. run the Flask app through Gunicorn
6. serve the frontend build through Nginx
7. proxy `/api/*` and `/health` to Gunicorn

Basic setup:

```bash
pip install -r requirements.txt
cd alarm_pred
npm install
npm run build
cd ..
```

Run the API directly:

```bash
python src/saas/app.py --host 0.0.0.0 --port 5000
```

Production-style Gunicorn command:

```bash
gunicorn --workers 4 --bind 127.0.0.1:5000 src.saas.app:app
```

Nginx should:

- serve static files from `alarm_pred/dist/`
- proxy `/api/` to `127.0.0.1:5000`
- proxy `/health` to `127.0.0.1:5000`
- return `index.html` for frontend routes

Data required for deployment:

- model files used by `src/forecast_pipeline/batch_predict.py`
- `data/final/FINAL_FEATURES_24H.csv` if you want to generate predictions on the server
- `data/predictions/predictions_latest.json` if predictions are already prepared

If you run forecasting on the same server, the usual order is:

1. collect and prepare input data
2. run `src/forecast_pipeline/merge_features.py`
3. run `src/forecast_pipeline/batch_predict.py`
4. let the API serve the latest JSON file

If you use cron, keep it simple and explicit. Do not reference helper scripts that are not present in the repository. A direct command is easier to maintain, for example:

```bash
10 * * * * cd /path/to/WarEvents && /path/to/venv/bin/python src/forecast_pipeline/batch_predict.py >> data/logs/cron.log 2>&1
```

For this repository, it is better to document the real commands you run than to keep large AWS-specific guides that may become outdated.
