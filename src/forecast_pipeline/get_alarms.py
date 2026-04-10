import os
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(ENV_PATH)

API_USER = os.getenv("API_USER")
API_KEY = os.getenv("API_KEY")

ALERTS_URL = "https://api.ukrainealarm.com/api/v3/alerts"
REGIONS_URL = "https://api.ukrainealarm.com/api/v3/regions"
HISTORY_FILE = ROOT_DIR / "data/alarms/alarms_history_raw.csv"

OBLAST_UA_TO_EN = {
    "Черкаська область": "Cherkasy", "Чернігівська область": "Chernihiv",
    "Чернівецька область": "Chernivtsi", "Дніпропетровська область": "Dnipro",
    "Донецька область": "Donetsk", "Івано-Франківська область": "Ivano-Frankivsk",
    "Харківська область": "Kharkiv", "Херсонська область": "Kherson",
    "Хмельницька область": "Khmelnytskyi", "Кіровоградська область": "Kropyvnytskyi",
    "Київська область": "Kyiv", "Волинська область": "Lutsk",
    "Львівська область": "Lviv", "Миколаївська область": "Mykolaiv",
    "Одеська область": "Odesa", "Полтавська область": "Poltava",
    "Рівненська область": "Rivne", "Сумська область": "Sumy",
    "Тернопільська область": "Ternopil", "Закарпатська область": "Uzhhorod",
    "Вінницька область": "Vinnytsia", "Запорізька область": "Zaporizhzhia",
    "Житомирська область": "Zhytomyr", "м. Київ": "Kyiv"
}

ALL_REGIONS = sorted(list(set(OBLAST_UA_TO_EN.values())))


def build_region_id_map() -> dict:
    headers = {"Authorization": f"{API_USER}:{API_KEY}"}
    try:
        r = requests.get(REGIONS_URL, headers=headers, timeout=10)
        r.raise_for_status()
        id_to_en = {}
        for state in r.json().get("states", []):
            oblast_en = OBLAST_UA_TO_EN.get(state["regionName"])
            if oblast_en:
                id_to_en[str(state["regionId"])] = oblast_en
                for district in state.get("regionChildIds", []):
                    id_to_en[str(district["regionId"])] = oblast_en
                    for community in district.get("regionChildIds", []):
                        id_to_en[str(community["regionId"])] = oblast_en
        return id_to_en
    except Exception as e:
        print(f"Error loading regions: {e}")
        return {}


def collect_alarms():
    if not API_USER or not API_KEY:
        print("ERROR: ALARM_API_USER or ALARM_API_KEY is missing.")
        return

    # tz-aware UTC — стандарт пайплайну
    now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    print(f"[{now_utc.isoformat()}] Fetching 5-min alarm snapshot...")

    id_map = build_region_id_map()
    if not id_map: return

    headers = {"Authorization": f"{API_USER}:{API_KEY}"}
    try:
        r = requests.get(ALERTS_URL, headers=headers, timeout=10)
        r.raise_for_status()
        alerts_data = r.json()
    except Exception as e:
        print(f"API Error: {e}")
        return

    # Визначаємо активні регіони
    active_regions = set()
    for region in alerts_data:
        oblast_en = id_map.get(str(region.get("regionId", "")))
        if oblast_en and any(a.get("type") == "AIR" for a in region.get("activeAlerts", [])):
            active_regions.add(oblast_en)

    # Формуємо поточний знімок
    new_rows = []
    for city in ALL_REGIONS:
        new_rows.append({
            "datetime": now_utc.isoformat(),
            "city": city,
            "alarm": 1 if city in active_regions else 0
        })
    df_new = pd.DataFrame(new_rows)

    # Читаємо історію, дописуємо нове, відрізаємо старе (залишаємо 48 годин)
    path = Path(HISTORY_FILE)
    if path.exists():
        df_hist = pd.read_csv(path)
    else:
        df_hist = pd.DataFrame()

    df_full = pd.concat([df_hist, df_new], ignore_index=True)
    # format='ISO8601' коректно обробляє і "+00:00", і naive рядки, і мішані формати
    df_full["datetime"] = pd.to_datetime(df_full["datetime"], format='ISO8601', utc=True)

    cutoff_time = now_utc - timedelta(hours=48)
    df_full = df_full[df_full["datetime"] >= cutoff_time]

    df_full = df_full.sort_values(["city", "datetime"]).drop_duplicates()

    path.parent.mkdir(parents=True, exist_ok=True)
    df_full.to_csv(path, index=False)
    print(f"Saved snapshot. Total history rows: {len(df_full)}")


if __name__ == "__main__":
    collect_alarms()