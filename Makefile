PYTHON ?= python3
VENV   := .venv
PIP    := $(VENV)/bin/pip
DBT    := $(VENV)/bin/dbt
DBT_FLAGS := --profiles-dir . --project-dir .

.PHONY: help venv setup deps build snapshot test docs all clean \
        airflow-up airflow-down trigger-pipeline trigger-refresh airflow-test

help:
	@echo "Targets:"
	@echo "  make venv             create .venv (Python 3.11)"
	@echo "  make setup            download MovieLens + generate users.csv"
	@echo "  make deps             install python deps + dbt deps"
	@echo "  make build            dbt seed + snapshot + run"
	@echo "  make test             dbt test"
	@echo "  make docs             dbt docs generate + serve"
	@echo "  make all              setup + deps + build + test"
	@echo "  make clean            remove data/, target/, dbt_packages/"
	@echo ""
	@echo "  make airflow-up       docker compose up -d"
	@echo "  make airflow-down     docker compose down -v"
	@echo "  make trigger-pipeline trigger lakehouse_daily_pipeline"

venv:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip

setup: venv
	$(PIP) install -r requirements.txt
	$(VENV)/bin/python setup.py

deps: venv
	$(PIP) install -r requirements.txt
	$(PIP) install vendor/pipeline_sentinel-0.1.0-py3-none-any.whl
	$(DBT) deps $(DBT_FLAGS)

build:
	$(DBT) seed     $(DBT_FLAGS)
	$(DBT) snapshot $(DBT_FLAGS)
	$(DBT) run      $(DBT_FLAGS)

snapshot:
	$(DBT) snapshot $(DBT_FLAGS)

test:
	$(DBT) test $(DBT_FLAGS)

docs:
	$(DBT) docs generate $(DBT_FLAGS)
	$(DBT) docs serve    $(DBT_FLAGS)

all: setup deps build test

clean:
	rm -rf data/*.duckdb data/ml-25m* target/ dbt_packages/ .pytest_cache logs/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ── Airflow targets ─────────────────────────────────────────────
airflow-up:
	docker compose up -d

airflow-down:
	docker compose down -v

trigger-pipeline:
	docker compose exec scheduler airflow dags trigger lakehouse_daily_pipeline

trigger-refresh:
	docker compose exec scheduler airflow dags trigger movielens_data_refresh

airflow-test:
	docker compose exec scheduler python -m pytest /usr/local/airflow/tests/ -v
