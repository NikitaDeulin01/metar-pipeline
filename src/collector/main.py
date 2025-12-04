import os
import datetime as dt
from typing import List, Dict, Any, Optional

import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

load_dotenv()

CHECKWX_API_KEY = os.getenv("CHECKWX_API_KEY")
CHECKWX_BASE_URL = os.getenv("CHECKWX_BASE_URL", "https://api.checkwx.com/metar")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "metar")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "metar_raw")

if not CHECKWX_API_KEY:
    raise RuntimeError("CHECKWX_API_KEY не задан в .env")

# Для проекта я выбрал топ-20 РФ аэропортов
RUS_TOP20_ICAO = [
    "UUEE",  # Шереметьево
    "UUDD",  # Домодедово
    "UUWW",  # Внуково
    "ULLI",  # Пулково
    "URSS",  # Сочи
    "USSS",  # Екб
    "UKFF",  # Симферополь
    "UNNT",  # Новосибирск
    "URKK",  # Красндодар
    "UWUU",  # Уфа
    "UWWW",  # Самара
    "UWKD",  # Казань
    "URRR",  # Ростов
    "UNKL",  # Красноярск
    "URMM",  # Мин. воды
    "UHWW",  # Владивосток
    "UHHH",  # Хабаровск
    "UIII",  # Иркутск
    "USTR",  # Тюмень
    "UMKK",  # Калининград
]


def get_mongo_collection() -> Collection:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]


def fetch_metar_decoded(icaos: List[str]) -> List[Dict[str, Any]]:
    """
    Запрашиваем METAR из CheckWX для списка ICAO.
    """
    icao_str = ",".join(icaos)
    url = f"{CHECKWX_BASE_URL}/{icao_str}/decoded"

    headers = {
        "X-API-Key": CHECKWX_API_KEY,
    }

    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()

    payload = resp.json()
    data = payload.get("data", [])
    return data


def _get_nested(d: Dict[str, Any], *path, default=None) -> Any:
    """
    достаём вложенные поля из словаря.
    """
    cur: Any = d
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def normalize_metar(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Нормализуем структуру
    """
    now_utc = dt.datetime.utcnow()

    temperature = record.get("temperature") or {}
    dewpoint = record.get("dewpoint") or {}
    wind = record.get("wind") or {}
    visibility = record.get("visibility") or {}
    barometer = record.get("barometer") or {}
    ceiling = record.get("ceiling") or {}
    humidity = record.get("humidity") or {}
    elevation = record.get("elevation") or {}
    station = record.get("station") or {}
    geometry = station.get("geometry") or {}
    coordinates = geometry.get("coordinates") or None

    # Достаём координаты
    station_lon: Optional[float] = None
    station_lat: Optional[float] = None
    if isinstance(coordinates, (list, tuple)) and len(coordinates) >= 2:
        station_lon = coordinates[0]
        station_lat = coordinates[1]

    doc: Dict[str, Any] = {
        # базовые поля, характеристика аэропорта, сырая строка метара
        "icao": record.get("icao"),
        "observed": record.get("observed"),  # строка времени наблюдения от CheckWX
        "raw_text": record.get("raw_text") or record.get("raw"),

        "flight_category": record.get("flight_category"),

        # температура
        "temperature_c": temperature.get("celsius"),
        "temperature_f": temperature.get("fahrenheit"),
        "dewpoint_c": dewpoint.get("celsius"),
        "dewpoint_f": dewpoint.get("fahrenheit"),

        # ветер
        "wind_dir_deg": wind.get("degrees"),
        "wind_speed_kt": wind.get("speed_kts"),
        "wind_speed_mps": wind.get("speed_mps"),
        "wind_speed_kph": wind.get("speed_kph"),
        "wind_speed_mph": wind.get("speed_mph"),
        "wind_gust_kt": wind.get("gust_kts"),

        # видимость
        "visibility_m": visibility.get("meters"),
        "visibility_m_text": visibility.get("meters_text"),
        "visibility_miles": visibility.get("miles"),
        "visibility_miles_text": visibility.get("miles_text"),

        # давление
        "barometer_hg": barometer.get("hg"),
        "barometer_hpa": barometer.get("hpa"),
        "barometer_kpa": barometer.get("kpa"),
        "barometer_mb": barometer.get("mb"),

        # тоже видимость
        "ceiling_feet": ceiling.get("feet"),
        "ceiling_meters": ceiling.get("meters"),

        # влажность
        "humidity_percent": humidity.get("percent"),

        # высота
        "elevation_feet": elevation.get("feet"),
        "elevation_meters": elevation.get("meters"),

        # информация о станции
        "station_name": station.get("name"),
        "station_location": station.get("location"),
        "station_type": station.get("type"),
        "station_lon": station_lon,
        "station_lat": station_lat,

        # облака
        "clouds": record.get("clouds"),
        "conditions": record.get("conditions"),

        # служебные поля
        "source": "checkwx",
        "inserted_at": now_utc,
    }

    return doc


def collect_and_store_once() -> int:
    raw_data = fetch_metar_decoded(RUS_TOP20_ICAO)

    if not raw_data:
        print("CheckWX вернул пустой список data")
        return 0

    docs = [normalize_metar(rec) for rec in raw_data]

    collection = get_mongo_collection()
    result = collection.insert_many(docs)

    print(f"Вставлено документов в Mongo: {len(result.inserted_ids)}")
    return len(result.inserted_ids)


def main():
    try:
        count = collect_and_store_once()
    except Exception as e:
        print(f"Ошибка при сборе METAR: {e}")


if __name__ == "__main__":
    main()
