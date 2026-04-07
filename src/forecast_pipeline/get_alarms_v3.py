import requests
import json
import os
from datetime import datetime, timezone
from pathlib import Path

API_USER   = "f1b937ba"
API_KEY    = "4ba92ffaa4b156cdc90661560dd48eac"
ALERTS_URL = "https://api.ukrainealarm.com/api/v3/alerts"
REGIONS_URL = "https://api.ukrainealarm.com/api/v3/regions"

RAW_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "data", "alarms", "alarms_raw.json")

OBLAST_UA_TO_EN = {
    "Черкаська область":          "Cherkasy",
    "Чернігівська область":       "Chernihiv",
    "Чернівецька область":        "Chernivtsi",
    "Дніпропетровська область":   "Dnipro",
    "Донецька область":           "Donetsk",
    "Івано-Франківська область":  "Ivano-Frankivsk",
    "Харківська область":         "Kharkiv",
    "Херсонська область":         "Kherson",
    "Хмельницька область":        "Khmelnytskyi",
    "Кіровоградська область":     "Kropyvnytskyi",
    "Київська область":           "Kyiv",
    "Волинська область":          "Lutsk",
    "Львівська область":          "Lviv",
    "Миколаївська область":       "Mykolaiv",
    "Одеська область":            "Odesa",
    "Полтавська область":         "Poltava",
    "Рівненська область":         "Rivne",
    "Сумська область":            "Sumy",
    "Тернопільська область":      "Ternopil",
    "Закарпатська область":       "Uzhhorod",
    "Вінницька область":          "Vinnytsia",
    "Запорізька область":         "Zaporizhzhia",
    "Житомирська область":        "Zhytomyr",
    "м. Київ":                    "Kyiv",
    "Луганська область":          "Luhansk",
}

ALL_REGIONS = [
    "Cherkasy", "Chernihiv", "Chernivtsi", "Dnipro", "Donetsk",
    "Ivano-Frankivsk", "Kharkiv", "Kherson", "Khmelnytskyi", "Kropyvnytskyi",
    "Kyiv", "Lutsk", "Lviv", "Mykolaiv", "Odesa", "Poltava", "Rivne",
    "Sumy", "Ternopil", "Uzhhorod", "Vinnytsia", "Zaporizhzhia", "Zhytomyr"
]


def build_region_id_map() -> dict:
    """
    Будує маппінг regionId → англійська назва області.
    Завантажує ієрархію з /regions і маппить кожен район/громаду
    до батьківської області.
    """
    headers = {"Authorization": f"{API_USER}:{API_KEY}"}
    try:
        r = requests.get(REGIONS_URL, headers=headers, timeout=10)
        r.raise_for_status()
        if not r.text:
            return {}
        data = r.json()
    except Exception as e:
        print(f"Error loading regions: {e}")
        return {}

    id_to_en = {}
    for state in data.get("states", []):
        oblast_ua   = state["regionName"]
        oblast_en   = OBLAST_UA_TO_EN.get(oblast_ua)
        if not oblast_en:
            continue

        id_to_en[str(state["regionId"])] = oblast_en

        for district in state.get("regionChildIds", []):
            id_to_en[str(district["regionId"])] = oblast_en

            for community in district.get("regionChildIds", []):
                id_to_en[str(community["regionId"])] = oblast_en

    print(f"Region map built: {len(id_to_en)} entries")
    return id_to_en


def get_current_alarms() -> list:
    headers = {"Authorization": f"{API_USER}:{API_KEY}"}
    try:
        r = requests.get(ALERTS_URL, headers=headers, timeout=10)
        r.raise_for_status()
        if not r.text:
            print("Empty response from API")
            return []
        return r.json()
    except Exception as e:
        print(f"API request error: {e}")
        return []


def parse_active_regions(alerts: list, id_map: dict) -> list:
    """
    Визначає які області мають активну AIR тривогу.
    Будь-який район/громада з AIR тривогою → вся область має тривогу.
    """
    active = set()
    for region in alerts:
        region_id  = str(region.get("regionId", ""))
        oblast_en  = id_map.get(region_id)
        if not oblast_en:
            continue
        alerts_list = region.get("activeAlerts", [])
        has_air = any(a.get("type") == "AIR" for a in alerts_list)
        if has_air:
            active.add(oblast_en)

    # фільтруємо тільки регіони які є в нашому датасеті
    return sorted([r for r in active if r in ALL_REGIONS])


def save_alarms(active_regions: list, raw_data: list):
    # Тут має бути RAW_OUTPUT_PATH
    path = Path(RAW_OUTPUT_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)

    records = []
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            records = json.load(f)

    record = {
        "collected_at":    datetime.now(timezone.utc).isoformat(),
        "active_regions":  active_regions,
        "active_count":    len(active_regions),
        "raw_api_data":    raw_data
    }
    records.append(record)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    # І тут має бути RAW_OUTPUT_PATH
    print(f"Saved to {RAW_OUTPUT_PATH}")


if __name__ == "__main__":
    print(f"Collecting alarms at {datetime.now(timezone.utc).isoformat()}...")

    id_map = build_region_id_map()
    if not id_map:
        print("Failed to build region map!")
        exit(1)

    alerts = get_current_alarms()
    if not alerts:
        print("No alerts data received!")
        exit(1)

    active = parse_active_regions(alerts, id_map)
    print(f"Active AIR alarms ({len(active)} regions): {active}")

    save_alarms(active, alerts)
