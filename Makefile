# Shortcuts. `make help` lists them.
# A Makefile is documentation that cannot go stale, because people run it.

.PHONY: help up down reset logs seed test lint fmt install hooks psql metabase-setup

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Install dev dependencies
	pip install -r requirements-dev.txt

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
	python -m data_generator.generate --rows 5000 --profile clean --seed 42
	python -m data_generator.generate --rows 5000 --profile messy --seed 42
	python -m data_generator.generate --rows 0 --profile empty --seed 42

test:  ## Run unit tests only (fast)
	pytest tests/unit -m unit

test-all:  ## Run everything (needs the stack up)
	pytest

lint:  ## Run every linter CI runs
	ruff check .
	black --check .
	yamllint .github/ config/ docker-compose.yml

fmt:  ## Auto-format
	ruff check --fix .
	black .

psql:  ## Open a psql shell on the analytics database
	docker compose exec postgres psql -U platform -d analytics

metabase-setup:  ## Connect Metabase to analytics and build the Sales Overview dashboard
	python scripts/setup_metabase.py
