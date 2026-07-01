.PHONY: help dev build publish test lint install clean clean-all

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

dev: ## Vite dev server (:1313) + proxen (:1212) with .py auto-restart
	uv run main.py dev

build: ## One-shot minified production build into proxen/dashboard/
	uv run main.py build

publish: ## install + build + uv build + uv publish
	uv run main.py publish

test: ## Run the pytest suite
	uv run pytest

lint: ## Run eslint on frontend/src
	uv run main.py lint

install: ## Install proxen as uv tool binary
	uv run main.py build
	uv tool install --force .

clean: ## Remove build artifacts and caches (keeps node_modules/.venv)
	rm -rf dist build *.egg-info .pytest_cache
	rm -f proxen/dashboard/app.js proxen/dashboard/app.css proxen/dashboard/meta.json
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

clean-all: clean ## Also remove node_modules and .venv
	rm -rf node_modules .venv
