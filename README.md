# War Events Prediction SaaS
Python SaaS to predict war events for the regions based on the weather forecast, situations in other regions, historical weather, alarms, and news data.

This repository contains the Data receiver modules responsible for collecting, parsing, and formatting raw data from various sources (Weather API, ISW, Reddit) for further forecasting.

## Scripts
All executable Python scripts are located in the `src/` directory:

* `get_weather_forecast.py` : сonnects to the Visual Crossing Weather API, selects target locations, and captures the 24-hour weather forecast in JSON format.
* `collect_isw.py` : performs web scraping on the ISW website, extracting historical information about all reports from 24.02.2022 to 03.03.2026, formatted as CSV/JSON.
* `collect_isw_daily.py` : performs daily web scraping on the ISW website to fetch the previous day's report.
* `reddit_zst_filter_zstandard.py` : processes large Reddit data dumps (`.zst` format) by subreddit based on parameters defined in `config.json`, outputting CSV files.


## Installation and setup

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd <your-repo-name>
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install browser binaries:**
   Playwright requires specific browser binaries to run. Install only Chromium to save space:
   ```bash
   playwright install chromium
   ```

## Usage

### Weather forecast
To get the 24-hour weather forecast, execute the following command from the root directory:

```bash
python src/get_weather_forecast.py
```

### ISW reports
To run the historical or daily ISW scraper:

```bash
python src/collect_isw.py
python src/collect_isw_daily.py
```
Processed data will be saved in the `data/` directory as .csv and .json formats.


### Reddit data processing
* Ensure the `input/` folder exists in the root directory and place your raw Reddit dumps (`.zst` format) inside it.
* Ensure the `config.json` file is present in the root directory to manage chunk sizes, logging, and subreddit filtering.

Run the processor:

Usage of the `zstandard` Python library requires a high amount of RAM (8GB+). This method is best suited for speed and error tracking.

```bash
python src/reddit_zst_filter_zstandard.py
```

Processed data will be saved in the `output/` directory.