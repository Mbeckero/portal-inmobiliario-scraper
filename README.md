# portal-inmobiliario-scraper

A production-grade web scraper for Chilean real estate listings from [Portal Inmobiliario](https://www.portalinmobiliario.com). Collects property data, tracks price changes over time, and exports to CSV, JSON, or Parquet.

## Features

- Scrapes search results and individual listing detail pages
- Tracks price and attribute changes across runs
- Exports to CSV, JSON, and Parquet
- Configuration-driven CSS selectors (easy to update when the site changes)
- Chilean-specific parsing: CLP/UF/USD currencies, commune slugs, all 16 regions
- Rate limiting with random delays and automatic retries
- Resume-capable: checkpoints allow restarting interrupted scrapes
- Docker support with optional daily scheduler

## Requirements

- Python 3.11+
- Node.js (for Playwright browser)
- Make

Or: Docker + Docker Compose (no local Python required)

## Quick Start

```bash
# Clone and set up
git clone <repo-url>
cd portal-inmobiliario-scraper
cp .env.example .env

# Install dependencies, init DB, install Playwright browser
make setup

# Quick test run (2 pages, 20 listings)
make run-sample
```

## Configuration

Copy `.env.example` to `.env` and adjust as needed:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `data/scraper.db` | SQLite database path |
| `HEADLESS` | `true` | Run browser headlessly |
| `MIN_DELAY` | `2.5` | Minimum delay between requests (seconds) |
| `MAX_DELAY` | `6.0` | Maximum delay between requests (seconds) |
| `MAX_PAGES` | `50` | Maximum search result pages to scrape |
| `MAX_LISTINGS` | `1000` | Maximum listings to scrape per run |
| `SAVE_RAW` | `false` | Save raw HTML/JSON for debugging |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

## CLI Usage

The `piscraper` CLI provides the following commands:

```bash
piscraper init-db                      # Initialize the database
piscraper scrape-search --search-url "https://..." --max-pages 10
piscraper scrape-details               # Fetch details for discovered listings
piscraper scrape-all --search-url "https://..." --max-pages 10 --max-listings 200
piscraper export --format csv --format json
piscraper detect-new --since 2024-01-15
piscraper doctor                       # Environment health check
```

### Makefile shortcuts

```bash
make setup                             # First-time setup
make run-sample                        # Test with 2 pages, 20 listings
make scrape-all ARGS="--search-url https://... --max-pages 20"
make export                            # Export to CSV + JSON
make test                              # Run test suite
make doctor                            # Health check
```

## Docker

```bash
docker compose build

# Single run
docker compose run --rm scraper scrape-all --search-url "https://..." --max-pages 5

# Background daily scheduler
docker compose --profile scheduler up -d
```

## Data Output

**Database** (`data/scraper.db`):
- `listings` — current state of each property
- `listing_runs` — historical snapshots per run (for change tracking)
- `scraper_runs` — metadata per execution

**Exports** (`data/exports/YYYY-MM-DD/`):
- `listings_all.csv` / `.json` — all active listings
- `listings_new_YYYY-MM-DD.csv` / `.json` — newly discovered listings

**Logs** (`data/logs/`): structured JSON logs; screenshots saved on page load failures.

## Project Structure

```
src/
  cli.py                        # CLI entry point (Typer)
  config.py                     # Settings (Pydantic + .env)
  models.py                     # Data models
  database.py                   # SQLAlchemy async ORM
  browser.py                    # Playwright browser manager
  discovery.py                  # URL builder, commune slug resolution
  normalization.py              # Price, area, region parsing
  dedupe.py                     # Change detection and fingerprinting
  exporters.py                  # CSV, JSON, Parquet export
  scheduler.py                  # Daily scheduler
  parsers/
    search_parser.py            # Search result page parsing
    detail_parser.py            # Listing detail page parsing
    structured_data_parser.py   # JSON-LD extraction
  pipelines/
    scrape_search.py            # Search discovery pipeline
    scrape_details.py           # Detail fetching pipeline
  repositories/
    listing_repository.py       # Database access layer
  services/
    portal_inmobiliario.py      # High-level site operations

configs/
  selectors.yaml                # CSS selectors (easy to update)
  defaults.yaml                 # Portal-specific defaults (communes, currencies, etc.)

tests/                          # pytest test suite
```

## Scraping Pipeline

1. **Search discovery** — visits paginated search result pages, extracts listing URLs
2. **Detail fetching** — visits each listing page, extracts full property data
3. **Change detection** — computes fingerprint (id + price + bedrooms + area + commune), records changes
4. **Export** — writes active listings to the configured output formats

## Testing

```bash
make test           # Run all tests
make test-fast      # Skip integration tests
```

## License

MIT
