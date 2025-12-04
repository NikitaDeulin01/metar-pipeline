# METAR

## Стек

- Python 3.11
- MongoDB + mongo-express
- PostgreSQL + pgAdmin
- Apache Airflow (CeleryExecutor)
- Docker Compose
- GitHub Actions (CI)

Запуск: docker compose -f infra/docker-compose.yml up -d
Доступы по умолчанию:
Airflow: http://localhost:8080
 (admin / admin)
Mongo Express: http://localhost:8081
 (admin / admin)
pgAdmin: http://localhost:5050
 (admin@admin.com / admin)
 
