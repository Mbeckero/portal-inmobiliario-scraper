# Portal Inmobiliario Scraper

Web scraper asíncrono para propiedades inmobiliarias chilenas de [portalinmobiliario.com](https://www.portalinmobiliario.com). Escrito en Python 3.11+ con Playwright, SQLAlchemy async, y FastAPI.

## Arquitectura

```
src/
├── cli.py                  # Punto de entrada CLI (Typer)
├── config.py               # Configuración con Pydantic Settings + .env
├── models.py               # Modelos de datos (Listing, SearchCard, ScraperRun)
├── database.py             # ORM async SQLAlchemy + tablas
├── browser.py              # Manejador de Playwright
├── discovery.py            # Constructor de URLs de búsqueda
├── normalization.py        # Parseo específico de Chile (precios, áreas, regiones)
├── dedupe.py               # Detección de cambios y fingerprinting
├── exporters.py            # Exportación CSV/JSON/Parquet
├── scheduler.py            # Scheduler diario (librería schedule)
├── web.py                  # Endpoints FastAPI para web UI
├── utils.py                # Utilidades de texto, hashing, IDs
├── logging_config.py       # Logging estructurado JSON (structlog)
├── parsers/
│   ├── search_parser.py    # Parseo de páginas de resultados (selectolax)
│   ├── detail_parser.py    # Parseo de páginas de detalle
│   └── structured_data_parser.py  # Extracción de JSON-LD
├── pipelines/
│   ├── scrape_search.py    # Pipeline de descubrimiento (multi-página)
│   ├── scrape_details.py   # Pipeline de fetch de detalles + persistencia
│   └── enrich.py           # Enriquecimiento de datos
├── repositories/
│   └── listing_repository.py  # Capa de acceso a DB (CRUD + queries)
└── services/
    └── portal_inmobiliario.py  # Operaciones de alto nivel del sitio
configs/
├── selectors.yaml          # Selectores CSS (configurables, fácil de mantener)
├── defaults.yaml           # Defaults del portal (comunas, monedas, regiones)
└── search_urls.txt         # URLs de búsqueda de ejemplo
```

## Pipeline de scraping

1. **Search Discovery** (`pipelines/scrape_search.py`) — Pagina resultados, extrae `SearchCard` ligeros
2. **Detail Fetching** (`pipelines/scrape_details.py`) — Visita cada listing, extrae `Listing` completo, detecta cambios, upserta en DB
3. **Export** (`exporters.py`) — Exporta a `data/exports/YYYY-MM-DD/`

## Comandos principales

```bash
# Setup inicial
make setup                          # Instala deps + Playwright + init DB

# Scraping
piscraper init-db                   # Inicializar SQLite
piscraper scrape-all --search-url "..." --max-pages 10 --max-listings 200
piscraper scrape-search --search-url "..."
piscraper scrape-details

# Exportación
piscraper export --format csv --format json
piscraper detect-new --since 2024-01-15

# Web UI
piscraper web --host 127.0.0.1 --port 8080

# Diagnóstico
piscraper doctor                    # Health check: Playwright, config, DB

# Desarrollo
make test                           # Correr pytest
make run-sample                     # Scrape de prueba (2 páginas, 20 listings)

# Docker
make docker-build
make docker-up
docker compose --profile scheduler up  # Con scheduler automático
```

## Tests

```bash
pytest tests/ -v                           # Todos los tests
pytest tests/ -v -m "not integration"     # Solo unit tests
pytest tests/test_normalization.py -v     # Tests específicos
```

**Archivos de test:**
- `tests/test_normalization.py` — Parseo de precios/áreas/regiones
- `tests/test_detail_parser.py` — Parseo de página de detalle
- `tests/test_search_parser.py` — Parseo de búsqueda
- `tests/test_dedupe.py` — Detección de cambios

## Linting y formato

```bash
ruff check src/ tests/    # Linting
ruff format src/ tests/   # Formateo
mypy src/                 # Type checking
```

## Configuración (.env)

Copiar `.env.example` a `.env` y ajustar:

```bash
DATABASE_URL=sqlite+aiosqlite:///data/scraper.db
HEADLESS=true
MIN_DELAY=2.5
MAX_DELAY=6.0
MAX_PAGES=50
MAX_LISTINGS=1000
SAVE_RAW=false
LOG_LEVEL=INFO
```

## Modelos de datos clave

- **`Listing`** — Propiedad completa (~50 campos): precio, moneda (CLP/UF/USD), dormitorios, baños, área m², región, comuna, coordenadas, vendedor, imágenes
- **`SearchCard`** — Vista ligera desde resultados de búsqueda
- **`ScraperRun`** — Metadata de ejecución (discovered, scraped, new, changed, failed)
- **`ListingChange`** — Historial de cambios por campo

## Convenciones de código

- **Async/await en todo** — Playwright y SQLAlchemy en modo async
- **Selectores en YAML** — `configs/selectors.yaml`, no hardcodeados en parsers
- **Normalización chilena** — Usar `src/normalization.py` para precios, áreas y regiones
- **Logging estructurado** — Usar `structlog` con contexto, no `print()`
- **Retry con tenacity** — Para operaciones de red falibles
- **Pydantic para config** — `src/config.py`, validación de env vars

## Base de datos

3 tablas SQLite:
1. **`listings`** — Estado actual de cada propiedad (upserted por `listing_id`)
2. **`listing_runs`** — Snapshots históricos por ejecución
3. **`scraper_runs`** — Metadata de cada ejecución del scraper
