PYTHON ?= python3.11
VENV   := .venv
PIP    := $(VENV)/bin/pip
DBT    := $(VENV)/bin/dbt
DBT_FLAGS := --profiles-dir . --project-dir .
PIP_DEPS := $(VENV)/.deps-installed
DBT_DEPS := dbt_packages

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

# venv + pip deps + observe wheel
$(PIP_DEPS): requirements.txt vendor/pipeline_observe-0.1.0-py3-none-any.whl
	rm -rf $(VENV)
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install vendor/pipeline_observe-0.1.0-py3-none-any.whl
	touch $(PIP_DEPS)

# dbt packages — re-run when packages.yml changes or dbt_packages/ is removed.
# order-only dep on the venv: needs dbt to exist, but a venv rebuild must not
# force a redundant dbt deps.
$(DBT_DEPS): packages.yml | $(PIP_DEPS)
	$(DBT) deps $(DBT_FLAGS)
	touch $(DBT_DEPS)

venv: $(PIP_DEPS) $(DBT_DEPS)

deps: $(PIP_DEPS) $(DBT_DEPS)

setup: $(PIP_DEPS) $(DBT_DEPS)
	$(VENV)/bin/python setup.py

# Order matters: dim_user_snapshot reads stg_users (silver), and gold dim_user
# reads the snapshot — so the snapshot must run AFTER its staging input exists
# and BEFORE the gold models that consume it. Build the staging chain, snapshot
# it, then build everything else.
build: $(PIP_DEPS) $(DBT_DEPS)
	$(DBT) seed                     $(DBT_FLAGS)
	$(DBT) run  --select +stg_users $(DBT_FLAGS)
	$(DBT) snapshot                 $(DBT_FLAGS)
	$(DBT) run                      $(DBT_FLAGS)

snapshot: $(PIP_DEPS) $(DBT_DEPS)
	$(DBT) snapshot $(DBT_FLAGS)

test: $(PIP_DEPS) $(DBT_DEPS)
	$(DBT) test $(DBT_FLAGS)

docs: $(PIP_DEPS) $(DBT_DEPS)
	$(DBT) docs generate $(DBT_FLAGS)
	$(DBT) docs serve    $(DBT_FLAGS)

all: setup deps build test

clean:
	rm -rf data/*.duckdb data/ml-25m* target/ dbt_packages/ .pytest_cache logs/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ── Airflow targets ─────────────────────────────────────────────
# Build the image before `up`: only airflow-init declares a build context, so on
# a fresh checkout a bare `docker compose up -d` tries to *pull* p2-airflow:latest
# for the webserver/scheduler/triggerer/dag-processor (which only reference it)
# and fails with "pull access denied / not found". Building airflow-init first
# tags the image locally so every service resolves it. (Mirrors project 2's
# build-first airflow-up; cached layers make repeat runs fast, and it also picks
# up requirements.txt changes that a bare `up -d` would silently ignore.)
airflow-up:
	docker compose build airflow-init
	docker compose up -d

airflow-down:
	docker compose down -v

trigger-pipeline:
	docker compose exec scheduler airflow dags trigger lakehouse_daily_pipeline

trigger-refresh:
	docker compose exec scheduler airflow dags trigger movielens_data_refresh

airflow-test:
	docker compose exec scheduler python -m pytest /usr/local/airflow/tests/ -v
