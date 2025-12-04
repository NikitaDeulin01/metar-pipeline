import os
from typing import List, Tuple, Any, Dict

from dotenv import load_dotenv
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# Mongo
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "metar")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "metar_raw")

# Postgres
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "airflow")
PG_USER = os.getenv("POSTGRES_USER", "airflow")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")


def get_mongo_docs() -> List[Dict[str, Any]]:
    """
    Читаем все документы из MongoDB коллекции metar_raw.
    """
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]
    docs = list(collection.find({}))
    print(f"Прочитано из Mongo документов: {len(docs)}")
    return docs


def get_pg_connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = False
    return conn


def ensure_table_exists(conn) -> None:
    """
    Создаём таблицу metar_observations
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS metar_observations (
        id                  SERIAL PRIMARY KEY,
        icao                TEXT NOT NULL,
        observed            TIMESTAMPTZ,
        flight_category     TEXT,
        temperature_c       REAL,
        dewpoint_c          REAL,
        wind_dir_deg        REAL,
        wind_speed_kt       REAL,
        wind_gust_kt        REAL,
        visibility_m        REAL,
        barometer_hpa       REAL,
        humidity_percent    REAL,
        station_name        TEXT,
        station_location    TEXT,
        station_lon         DOUBLE PRECISION,
        station_lat         DOUBLE PRECISION,
        raw_text            TEXT,
        inserted_at         TIMESTAMPTZ
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    conn.commit()
    print("Проверка / создание таблицы metar_observations выполнена.")


def transform_docs_for_insert(docs: List[Dict[str, Any]]) -> List[Tuple]:
    rows: List[Tuple] = []

    for d in docs:
        row = (
            d.get("icao"),
            d.get("observed"),
            d.get("flight_category"),
            d.get("temperature_c"),
            d.get("dewpoint_c"),
            d.get("wind_dir_deg"),
            d.get("wind_speed_kt"),
            d.get("wind_gust_kt"),
            d.get("visibility_m"),
            d.get("barometer_hpa"),
            d.get("humidity_percent"),
            d.get("station_name"),
            d.get("station_location"),
            d.get("station_lon"),
            d.get("station_lat"),
            d.get("raw_text"),
            d.get("inserted_at"),
        )
        rows.append(row)

    print(f"Подготовлено строк для вставки в Postgres: {len(rows)}")
    return rows


def insert_into_postgres(conn, rows: List[Tuple]) -> None:
    """
    Вставка данных в Postgres через execute_values.
    """
    if not rows:
        print("Нет данных для вставки в Postgres.")
        return

    insert_sql = """
        INSERT INTO metar_observations (
            icao,
            observed,
            flight_category,
            temperature_c,
            dewpoint_c,
            wind_dir_deg,
            wind_speed_kt,
            wind_gust_kt,
            visibility_m,
            barometer_hpa,
            humidity_percent,
            station_name,
            station_location,
            station_lon,
            station_lat,
            raw_text,
            inserted_at
        )
        VALUES %s;
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows)
    conn.commit()
    print(f"Вставлено строк в Postgres: {len(rows)}")


def run_el() -> None:
    docs = get_mongo_docs()
    rows = transform_docs_for_insert(docs)

    conn = get_pg_connection()
    try:
        ensure_table_exists(conn)
        insert_into_postgres(conn, rows)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        run_el()
    except Exception as e:
        print(f"Ошибка в EL-процессе: {e}")
