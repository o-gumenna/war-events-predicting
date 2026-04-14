## Notebooks overview

The `notebooks/` folder contains seven Jupyter notebooks used for data exploration, feature engineering, and model training. They are not part of the production cron pipeline but serve as the research foundation for the scripts in `src/forecast_pipeline/`.

There are two kinds of notebooks:

- source notebooks that process raw data and produce feature CSV files
- training notebooks that consume those feature files and produce trained models

Data files are expected under `data/`, which is not tracked in the repository. Raw source files must be collected using the scripts in `src/data_collection/` or obtained separately. Intermediate feature CSVs are produced by running the notebooks in order. None of these files are available in the repository or in public storage.


## Data flow

Each notebook reads specific input files and writes output files that are consumed by the next step:

### war_events.ipynb

Exploratory analysis of air alarm data across Ukrainian regions from February 24, 2022 to present. Examines alarm frequency, duration, and regional patterns. Produces lag and rolling-mean features used in the merged dataset.

Inputs:

- `alarms-merged.csv` — alarm records with region, start time, end time, duration; not tracked
- `regions.csv` — region reference table with names and codes; not tracked, must be obtained separately

Output:

- `alarms_hourly.csv` with per-region lag and rolling-mean columns


### isw_eda.ipynb

Transforms raw HTML text from daily ISW conflict assessment reports into a structured numerical feature dataset. Covers reports from February 24, 2022 to present.

Input:

- `isw_reports.csv` — one row per day with fields: date, url, full_text, word_count; not tracked, produced by `src/data_collection/collect_isw.py`

Output:

- `isw_features_data.csv` — 1,469 rows × 62 columns, one row per day, no missing values

Key steps: TF-IDF bigram matrix, keyword-based extraction of 9 military threat categories, binary geographic coverage features for 23 Ukrainian regions, lag-1 and lag-3 and 7-day rolling mean features for all threat categories.


### weather_EDA_final.ipynb

EDA and feature engineering for hourly weather data across 24 Ukrainian cities. Cleans anomalies, handles missing values, aggregates to daily granularity, and standardizes city names for merging with other datasets.

Input:

- `all_weather_by_hour_2023-2025_v1.csv` hourly weather CSVs covering February 24, 2022 to March 1, 2025, 24 cities; not tracked

Output:

- `weather_features_hourly.csv` with daily aggregated columns


### telegram_analysis.ipynb

Processes a Telegram channel JSON export from war-related channels. Extracts threat type and geographic features for use in the model.

Input:

- Telegram JSON export (`result.json`), messages from February 24, 2022 onward; not tracked

Output:

- `telegram_features_hourly.csv` — threat features, cities, weapon type count per message

Key steps: UTC to Kyiv timezone conversion with DST handling, text cleaning, city dictionary for 23 regional centers using regex, oblast name expansion, row duplication for messages mentioning multiple cities, threat dictionary filtering, `tg_threat_type_count` feature.


### reddit_analysis.ipynb

Analyzes posts from Ukraine-related subreddits. Cleans the data and produces daily aggregated activity and sentiment features.

Input:

- `reddit_ukraine.csv` - filtered Reddit ZST archive; not tracked, produced by `src/data_collection/reddit_zst_filter_zstandard.py` from a raw Pushshift dump

Output:

- `reddit_features_hourly.csv` — daily aggregated activity features


### merged_EDA_training.ipynb

Merges all five feature sources into a single dataset and performs final feature selection before model training.

Inputs:

- `alarms_hourly.csv`
- `isw_features_data.csv`
- `weather_features_hourly.csv`
- `telegram_features_hourly.csv`
- `reddit_features_hourly.csv`

Output:

- `merged_cleaned_dataset.csv` — final dataset ready for training

Key steps: sparsity analysis, alarm rate analysis, feature correlation with alarm probability, multicollinearity check, lag features for Telegram and Reddit, removal of correlated and low-signal features.


### model_training_pipeline.ipynb

Main training notebook. Reads the merged dataset, trains and compares several models for alarm prediction, and saves the trained model files used by the production pipeline.

Input:

- `merged_cleaned_dataset.csv`

Outputs:

- trained model files (`.pkl`) consumed by `src/forecast_pipeline/batch_predict.py`
- evaluation metrics and model comparison results

Key steps: chronological sort, target variable definition, feature group definition (Base, ISW, Telegram, Reddit, Weather), leakage removal, walk-forward validation with time series split, training Decision Tree and Random Forest and HistGradientBoosting, binary classification and duration regression, hyperparameter tuning with RandomizedSearchCV.


## Connection to the production pipeline

Each production script in `src/forecast_pipeline/` mirrors the logic of one or more notebooks:

- `get_alarms.py` mirrors `war_events.ipynb`
- `isw_fetch.py` mirrors `isw_eda.ipynb`
- `weather_forecast.py` mirrors `weather_EDA_final.ipynb`
- `telegram_fetch.py` mirrors `telegram_analysis.ipynb`
- `process_alarms_final.py` mirrors `war_events.ipynb`
- `merge_features.py` mirrors `merged_EDA_training.ipynb`
- `batch_predict.py` uses the model files produced by `model_training_pipeline.ipynb`

The notebooks are the place to re-examine data, retrain models, or debug unexpected prediction behavior. Once a model is retrained, copy the output files to `data/models/` so the production scripts can pick them up.


## Running order

If you want to reproduce the full dataset and retrain from scratch:

1. make sure raw source files are available under `data/raw/`
2. run `war_events.ipynb`
3. run `isw_eda.ipynb`
4. run `weather_EDA_final.ipynb`
5. run `telegram_analysis.ipynb`
6. run `reddit_analysis.ipynb`
7. run `merged_EDA_training.ipynb`
8. run `model_training_pipeline.ipynb`
9. copy trained model files to `data/models/`

If `FINAL_FEATURES_24H.csv` is already available, you can skip steps 2 through 7 and go straight to model training.