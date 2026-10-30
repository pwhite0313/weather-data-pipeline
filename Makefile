SCHEDULER = docker compose exec airflow-scheduler
DBT = $(SCHEDULER) bash -c "cd /opt/airflow/dbt && dbt
DT ?= $(shell date -u +%Y-%m-%d)

.PHONY: up down restart logs trigger runs load dbt-run dbt-test dbt-fresh test

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f airflow-scheduler

trigger:
	$(SCHEDULER) airflow dags trigger weather_daily

runs:
	$(SCHEDULER) airflow dags list-runs -d weather_daily -o table

# COPY one partition from the S3 stage into RAW.WEATHER_FORECAST.
# Safe to repeat: load metadata skips files already loaded (ADR 0001).
load:
	$(SCHEDULER) python -m src.snowflake_loader --dt $(DT)

dbt-run:
	$(DBT) run --profiles-dir /opt/airflow/dbt"

dbt-test:
	$(DBT) test --profiles-dir /opt/airflow/dbt"

dbt-fresh:
	$(DBT) source freshness --profiles-dir /opt/airflow/dbt"

test:
	python -m pytest tests/ -q
