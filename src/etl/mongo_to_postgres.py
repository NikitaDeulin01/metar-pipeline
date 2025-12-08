import os
import json
from typing import List, Dict, Any, Tuple

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

# Сырая таблица с JSON
RAW_TABLE = "metar_raw_json"


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


def ensure_raw_table_exists(conn) -> None:
    """
    Создаём сырую таблицу для JSON
    id
    payload
    inserted_at
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS public.{RAW_TABLE} (
        id          TEXT PRIMARY KEY,
        payload     JSONB NOT NULL,
        inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    conn.commit()
    # print(f"Проверка / создание таблицы public.{RAW_TABLE} выполнена.")


def transform_docs_for_insert(docs: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """
    Готовим данные к вставке: берём целиком документ и сериализуем его в JSON-строку.
    """
    rows: List[Tuple[str, str]] = []

    for d in docs:
        doc_id = str(d.get("_id"))

        # Сериализуем весь документ в JSON.
        payload_str = json.dumps(d, default=str)

        rows.append((doc_id, payload_str))

    # print(f"Подготовлено строк для вставки в Postgres (raw json): {len(rows)}")
    return rows


def insert_into_postgres(conn, rows: List[Tuple[str, str]]) -> None:
    """
    Вставка данных в Postgres в сырую таблицу (id + payload jsonb).
    """
    if not rows:
        print("Нет данных для вставки в Postgres.")
        return

    insert_sql = f"""
        INSERT INTO public.{RAW_TABLE} (
            id,
            payload
        )
        VALUES %s
        ON CONFLICT (id) DO UPDATE
        SET payload = EXCLUDED.payload,
            inserted_at = now()
        ;
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows)
    conn.commit()
    print(f"Вставлено/обновлено строк в Postgres (таблица {RAW_TABLE}): {len(rows)}")


def run_el() -> None:
    """
    EL
    """
    docs = get_mongo_docs()
    rows = transform_docs_for_insert(docs)

    conn = get_pg_connection()
    try:
        ensure_raw_table_exists(conn)
        insert_into_postgres(conn, rows)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        run_el()
    except Exception as e:
        print(f"Ошибка в EL-процессе: {e}")
