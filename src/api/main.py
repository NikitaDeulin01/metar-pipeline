# src/api/main.py
from typing import List, Any
from pathlib import Path
import subprocess
import sys
import os

from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bson import ObjectId

# импортируем collector
from src.collector.main import (
    collect_and_store_once,
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION,
)

BASE_DIR = Path(__file__).resolve().parents[2]
DBT_DIR = BASE_DIR / "dbt"

VENV_PYTHON = BASE_DIR / ".venv" / "Scripts" / "python.exe"
EDR_BIN = BASE_DIR / ".venv" / "Scripts" / "edr.exe"

app = FastAPI(
    title="METAR Collector API",
    description="HTTP-сервис для сбора METAR + dbt + Elementary",
    version="1.0.0",
)


def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]


def serialize_doc(doc: dict) -> dict:
    """
    Переводим ObjectId и даты в сериализуемый вид.
    """
    doc = dict(doc)
    if "_id" in doc and isinstance(doc["_id"], ObjectId):
        doc["_id"] = str(doc["_id"])
    return doc


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/collect")
def collect():
    """
    Триггерим сбор METAR
    """
    try:
        inserted_count = collect_and_store_once()
        return {"inserted": inserted_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при сборе METAR: {e}")


@app.get("/metar/latest")
def get_latest(limit: int = 20):
    """
    Отдаём последние N записей из Mongo.
    """
    try:
        coll = get_mongo_collection()
        cursor = (
            coll.find()
            .sort("inserted_at", -1)
            .limit(limit)
        )
        docs: List[Any] = [serialize_doc(d) for d in cursor]
        return docs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при чтении из Mongo: {e}")


@app.post("/dbt/build")
def run_dbt_build():
    """
    Локальный запуск dbt build
    """
    try:
        python_bin = VENV_PYTHON if VENV_PYTHON.exists() else Path(sys.executable)

        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(DBT_DIR)

        result = subprocess.run(
            [
                str(python_bin),
                "-m",
                "dbt.cli.main",
                "build",
                "--project-dir",
                str(DBT_DIR),
                "--profiles-dir",
                str(DBT_DIR),
            ],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            env=env,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout[-4000:],
            "stderr": result.stderr[-4000:],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при запуске dbt build: {e}")


@app.post("/dbt/report")
def run_elementary_report():
    """
    Локальный запуск Elementary отчёта
    """
    try:
        env = os.environ.copy()
        env["DBT_PROFILES_DIR"] = str(DBT_DIR)

        if EDR_BIN.exists():
            cmd = [str(EDR_BIN), "report"]
        else:
            python_bin = VENV_PYTHON if VENV_PYTHON.exists() else Path(sys.executable)
            cmd = [str(python_bin), "-m", "elementary.cli", "report"]

        result = subprocess.run(
            cmd,
            cwd=DBT_DIR,
            capture_output=True,
            text=True,
            env=env,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout[-4000:],
            "stderr": result.stderr[-4000:],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при запуске Elementary report: {e}")
