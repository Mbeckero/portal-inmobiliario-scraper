.PHONY: install install-dev playwright-install run-sample scrape-all export detect-new init-db doctor test lint fmt clean docker-build docker-up docker-down help

PYTHON := python3
PIP    := pip

# ── Setup ─────────────────────────────────────────────────────────────────────

install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev]"

playwright-install:
	playwright install chromium --with-deps

setup: install playwright-install
	cp -n .env.example .env || true
	$(PYTHON) -m src.cli init-db
	@echo "Setup complete. Edit .env then run: make run-sample"

# ── Running ───────────────────────────────────────────────────────────────────

init-db:
	$(PYTHON) -m src.cli init-db

run-sample:
	$(PYTHON) -m src.cli scrape-all \
		--search-url "https://www.portalinmobiliario.com/venta/departamento/las-condes-metropolitana" \
		--max-pages 2 \
		--max-listings 20

scrape-all:
	$(PYTHON) -m src.cli scrape-all $(ARGS)

export:
	$(PYTHON) -m src.cli export --format csv --format json

detect-new:
	$(PYTHON) -m src.cli detect-new

doctor:
	$(PYTHON) -m src.cli doctor

# ── Testing ───────────────────────────────────────────────────────────────────

test:
	pytest tests/ -v

test-fast:
	pytest tests/ -v -m "not integration"

# ── Code quality ──────────────────────────────────────────────────────────────

lint:
	ruff check src/ tests/

fmt:
	ruff format src/ tests/

# ── Docker ────────────────────────────────────────────────────────────────────

docker-build:
	docker compose build

docker-up:
	docker compose up

docker-run-sample:
	docker compose run --rm scraper make run-sample

docker-down:
	docker compose down

# ── Maintenance ───────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache .mypy_cache

help:
	@echo "Portal Inmobiliario Scraper — available commands:"
	@echo ""
	@echo "  make setup             Full first-time setup"
	@echo "  make run-sample        Run a small sample scrape"
	@echo "  make scrape-all        Run full scrape (pass ARGS=...)"
	@echo "  make export            Export to CSV and JSON"
	@echo "  make detect-new        Show new listings since last run"
	@echo "  make doctor            Check environment health"
	@echo "  make test              Run test suite"
	@echo "  make docker-up         Start with Docker Compose"
	@echo ""
