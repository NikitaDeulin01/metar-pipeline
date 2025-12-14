# METAR
METAR Data Pipeline — это пайплайн для автоматизированного сбора, хранения, трансформации и анализа метеорологических данных (METAR) для крупнейших аэропортов Российской Федерации.

## Архитектура пайплайна
```
METAR API
   ↓
Collector (Python)
   ↓
MongoDB
   ↓
EL (Mongo → PostgreSQL)
   ↓
PostgreSQL
   ↓
dbt 
   ↓
Elementary
   ↓
HTML Dashboard
```
## Стек

- Python 3.11
- MongoDB — хранение сырых METAR-данных
- PostgreSQL — аналитическое хранилище
- Apache Airflow — оркестрация пайплайна
- dbt — трансформации и тесты
- Elementary — мониторинг качества данных
- Docker & Docker Compose
- GitHub Actions — CI
- pre-commit / Ruff / SQLFluff — качество кода

Запуск: docker compose -f infra/docker-compose.yml up -d
Доступы по умолчанию:
- Airflow: http://localhost:8080 (admin / admin)
- Mongo Express: http://localhost:8081 (admin / admin)
- pgAdmin: http://localhost:5050 (admin@admin.com / admin)
  
 ## Структура проекта
```
metar-pipeline/
├── src/
│   ├── api/
│   │   └── main.py              # API
│   ├── collector/
│   │   └── main.py              # Сбор METAR и запись в MongoDB
│   └── etl/
│       └── mongo_to_postgres.py # MongoDB to PostgreSQL
│
├── dbt/
│   ├── models/
│   │   ├── sources/
│   │   │   └── sources.yml
│   │   ├── stg/
│   │   │   ├── stg_metar_observations.sql
│   │   │   └── schema.yml
│   │   ├── ods/
│   │   │   ├── ods_metar_latest.sql
│   │   │   └── schema.yml
│   │   ├── int/
│   │   │   ├── int_metar_latest.sql
│   │   │   └── schema.yml
│   │   └── dwh/
│   │       ├── dw_daily_airport_metrics.sql
│   │       └── schema.yml
│   ├── tests/
│   │   └── not_negative_visibility.sql
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── infra/
│   ├── docker-compose.yml
│   ├── .env
│   └── airflow/
│       └── dags/
│           └── metar_pipeline_dag.py
│
├── .github/workflows/ci.yml
├── .pre-commit-config.yaml
├── requirements.txt
├── .gitignore
└── README.md
```
