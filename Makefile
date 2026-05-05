.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

sync: ## Install dependencies with uv
	uv sync

run: openapi ## Start the app with uvicorn
	uv run uvicorn climate_api.main:app --reload

lint: ## Check linting, formatting, and types (no autofix)
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy src/
	uv run pyright

fix: ## Autofix ruff lint and format issues
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run tests with pytest
	uv run pytest tests/

openapi: ## Generate pygeoapi OpenAPI spec
	@set -a && . ./.env && set +a && \
		PYTHONPATH="$(PWD)/src" uv run python -c "from climate_api.publications.services import ensure_pygeoapi_base_config; ensure_pygeoapi_base_config()"

start: openapi ## Start the Docker stack (builds images first)
	docker compose up --build

restart: openapi ## Tear down, rebuild, and start the Docker stack from scratch
	docker compose down -v && docker compose build --no-cache && docker compose up
