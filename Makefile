PYTHON ?= python3.11
VENV   := .venv
PIP    := $(VENV)/bin/pip
DBT    := $(VENV)/bin/dbt
DBT_FLAGS := --profiles-dir . --project-dir .
OBSERVE := $(VENV)/.deps-installed

.PHONY: help venv setup deps build snapshot test docs all clean \
        airflow-up airflow-down trigger-pipeline trigger-refresh airflow-test

help:
	@echo "Targets:"
	@echo "  make venv             create .venv (Python 3.11) + install deps + dbt deps"
	@echo "  make setup            download MovieLens + generate users.csv (auto-creates .venv)"
	@echo "  make deps             alias for venv (install python deps + dbt deps)"
	@echo "  make build            dbt seed + snapshot + run (auto-creates .venv)"
	@echo "  make test             dbt test (auto-creates .venv)"
	@echo "  make docs             dbt docs generate + serve (auto-creates .venv)"
	@echo "  make all              setup + deps + build + test"
	@echo "  make clean            remove data/, target/, dbt_packages/"
	@echo ""
	@echo "  make airflow-up       docker compose up -d"
	@echo "  make airflow-down     docker compose down -v"
	@echo "  make trigger-pipeline trigger lakehouse_daily_pipeline"

$(OBSERVE):
	rm -rf $(VENV)
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install vendor/pipeline_observe-0.1.0-py3-none-any.whl
	$(DBT) deps $(DBT_FLAGS)
	touch $(OBSERVE)

venv: $(OBSERVE)

deps: $(OBSERVE)

setup: $(OBSERVE)
	$(VENV)/bin/python setup.py

build: $(OBSERVE)
	$(DBT) seed     $(DBT_FLAGS)
	$(DBT) snapshot $(DBT_FLAGS)
	$(DBT) run      $(DBT_FLAGS)

snapshot: $(OBSERVE)
	$(DBT) snapshot $(DBT_FLAGS)

test: $(OBSERVE)
	$(DBT) test $(DBT_FLAGS)

docs: $(OBSERVE)
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
