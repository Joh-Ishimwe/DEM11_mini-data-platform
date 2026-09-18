# Shortcuts. `make help` lists them.
# A Makefile is documentation that cannot go stale, because people run it.

# Recipes below use POSIX shell syntax (test, ||, subshells). GNU Make only
# runs them through a real shell if it can find one - invoked from Git Bash
# that's automatic, but invoked from PowerShell/cmd.exe (which don't have
# sh.exe on PATH) Make silently falls back to cmd.exe, which chokes on this
# syntax. Force Git Bash explicitly so `make` behaves the same from any shell.
ifeq ($(OS),Windows_NT)
SHELL := C:/Program Files/Git/bin/bash.exe
.SHELLFLAGS := -c
endif

# Resolve the project venv's interpreter so every target works whether or not
# the caller remembered to activate it - `python` on PATH is otherwise
# whatever the OS default is, which has none of requirements*.txt installed.
VENV_PYTHON := $(wildcard .venv/Scripts/python.exe .venv/bin/python)
ifeq ($(VENV_PYTHON),)
PYTHON := python
else
PYTHON := $(firstword $(VENV_PYTHON))
endif

.PHONY: help up down reset logs seed seed-minio test test-all lint fmt install hooks psql metabase-setup

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install:  ## Install dev dependencies
	$(PYTHON) -m pip install -r requirements-dev.txt

hooks:  ## Install pre-commit hooks
	pre-commit install

up:  ## Start the whole platform
	@test -f .env || (echo "No .env file. Run: cp .env.example .env" && exit 1)
	docker compose up -d
	@echo "Airflow  http://localhost:8080"
	@echo "MinIO    http://localhost:9001"
	@echo "Metabase http://localhost:3000"

down:  ## Stop, keeping data
	docker compose down

reset:  ## Stop and DELETE ALL DATA, then start fresh
	docker compose down -v
	docker compose up -d

logs:  ## Tail all logs (use: make logs s=airflow-scheduler)
	docker compose logs -f $(s)

seed:  ## Generate sample files into data/samples/
	$(PYTHON) -m data_generator.generate --rows 5000 --profile clean --seed 42
	$(PYTHON) -m data_generator.generate --rows 5000 --profile messy --seed 42
	$(PYTHON) -m data_generator.generate --rows 0 --profile empty --seed 42

seed-minio:  ## Generate sample files and upload them straight into MinIO's raw bucket
	@test -f .env || (echo "No .env file. Run: cp .env.example .env" && exit 1)
	set -a; . ./.env; set +a; \
	MINIO_ENDPOINT=http://localhost:9000 $(PYTHON) -m data_generator.generate --rows 5000 --profile clean --seed 42 --upload; \
	MINIO_ENDPOINT=http://localhost:9000 $(PYTHON) -m data_generator.generate --rows 5000 --profile messy --seed 42 --upload

test:  ## Run unit tests only (fast)
	$(PYTHON) -m pytest tests/unit -m unit

test-all:  ## Run everything (needs the stack up)
	$(PYTHON) -m pytest

lint:  ## Run every linter CI runs
	$(PYTHON) -m ruff check .
	$(PYTHON) -m black --check .
	$(PYTHON) -m yamllint .github/ config/ docker-compose.yml

fmt:  ## Auto-format
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m black .

psql:  ## Open a psql shell on the analytics database
	docker compose exec postgres psql -U platform -d analytics

metabase-setup:  ## Connect Metabase to analytics and build the Sales Overview dashboard
	$(PYTHON) scripts/setup_metabase.py
