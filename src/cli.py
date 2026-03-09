"""Typer CLI for Portal Inmobiliario Scraper.

Commands:
    init-db         Initialize the SQLite database
    scrape-search   Collect listing URLs from search pages only
    scrape-details  Visit detail pages for a set of listing URLs
    scrape-all      Full pipeline: search → details → export
    export          Export listings to CSV/JSON/Parquet
    detect-new      Show new listings discovered since the last run
    doctor          Check environment health
"""
from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table

from src.config import Settings, get_settings
from src.logging_config import configure_logging, get_logger

app = typer.Typer(
    name="piscraper",
    help="Portal Inmobiliario scraper — collect, normalize, and store Chilean real-estate listings.",
    add_completion=False,
)
console = Console()
logger = get_logger(__name__)


# ── Shared option helpers ─────────────────────────────────────────────────────

def _make_settings_override(
    headless: bool,
    min_delay: float,
    max_delay: float,
    save_raw: bool,
    database_url: Optional[str],
    log_level: str,
) -> Settings:
    cfg = get_settings()
    # Create a copy with overrides
    overrides: dict = {
        "headless": headless,
        "min_delay": min_delay,
        "max_delay": max_delay,
        "save_raw": save_raw,
        "log_level": log_level,
    }
    if database_url:
        overrides["database_url"] = database_url
    return cfg.model_copy(update=overrides)


# ── init-db ───────────────────────────────────────────────────────────────────

@app.command("init-db")
def cmd_init_db(
    database_url: Optional[str] = typer.Option(None, "--database-url", envvar="DATABASE_URL"),
    log_level: str = typer.Option("INFO", "--log-level", envvar="LOG_LEVEL"),
) -> None:
    """Create the SQLite database and all tables."""
    configure_logging(level=log_level)
    cfg = get_settings()
    url = database_url or cfg.database_url
    asyncio.run(_init_db_async(url))
    console.print(f"[green]Database initialized:[/green] {url}")


async def _init_db_async(database_url: str) -> None:
    from src.database import init_db
    await init_db(database_url)


# ── scrape-search ─────────────────────────────────────────────────────────────

@app.command("scrape-search")
def cmd_scrape_search(
    search_url: Optional[List[str]] = typer.Option(None, "--search-url"),
    search_urls_file: Optional[Path] = typer.Option(None, "--search-urls-file"),
    max_pages: int = typer.Option(10, "--max-pages"),
    max_listings: int = typer.Option(200, "--max-listings"),
    headless: bool = typer.Option(True, "--headless/--no-headless"),
    min_delay: float = typer.Option(2.5, "--min-delay"),
    max_delay: float = typer.Option(6.0, "--max-delay"),
    save_raw: bool = typer.Option(True, "--save-raw/--no-save-raw"),
    log_level: str = typer.Option("INFO", "--log-level"),
    output_dir: Optional[Path] = typer.Option(None, "--output-dir"),
) -> None:
    """Collect listing URLs from search result pages (no detail visits)."""
    configure_logging(level=log_level)
    urls = _resolve_search_urls(search_url, search_urls_file)
    if not urls:
        console.print("[red]Error:[/red] Provide at least one --search-url or --search-urls-file.")
        raise typer.Exit(1)

    cfg = get_settings()
    if output_dir:
        cfg = cfg.model_copy(update={"output_dir": output_dir})

    run_id = _new_run_id()
    cards = asyncio.run(
        _search_async(
            search_urls=urls,
            run_id=run_id,
            settings=cfg,
            max_pages=max_pages,
            max_listings=max_listings,
        )
    )
    console.print(f"[green]Discovered[/green] {len(cards)} listings. Run ID: {run_id}")


async def _search_async(
    search_urls: list[str],
    run_id: str,
    settings: Settings,
    max_pages: int,
    max_listings: int,
) -> list:
    from src.pipelines.scrape_search import run_search_pipeline
    return await run_search_pipeline(
        search_urls=search_urls,
        run_id=run_id,
        settings=settings,
        max_pages=max_pages,
        max_listings=max_listings,
    )


# ── scrape-all ────────────────────────────────────────────────────────────────

@app.command("scrape-all")
def cmd_scrape_all(
    search_url: Optional[List[str]] = typer.Option(None, "--search-url"),
    search_urls_file: Optional[Path] = typer.Option(None, "--search-urls-file"),
    operation_type: str = typer.Option("sale", "--operation-type", help="sale or rent"),
    property_type: Optional[str] = typer.Option(None, "--property-type"),
    region: Optional[str] = typer.Option(None, "--region"),
    commune: Optional[str] = typer.Option(None, "--commune"),
    max_pages: int = typer.Option(10, "--max-pages"),
    max_listings: int = typer.Option(200, "--max-listings"),
    headless: bool = typer.Option(True, "--headless/--no-headless"),
    min_delay: float = typer.Option(2.5, "--min-delay"),
    max_delay: float = typer.Option(6.0, "--max-delay"),
    save_raw: bool = typer.Option(True, "--save-raw/--no-save-raw"),
    output_dir: Optional[Path] = typer.Option(None, "--output-dir"),
    database_url: Optional[str] = typer.Option(None, "--database-url"),
    log_level: str = typer.Option("INFO", "--log-level", envvar="LOG_LEVEL"),
) -> None:
    """Full pipeline: search pages → detail pages → database → exports."""
    configure_logging(level=log_level)

    urls = _resolve_search_urls(search_url, search_urls_file)

    # Generate URL from --commune / --property-type if no direct URL given
    if not urls and commune:
        from src.discovery import build_search_url
        urls = [
            build_search_url(
                operation=operation_type,
                property_type=property_type or "apartment",
                location=commune,
            )
        ]

    if not urls:
        console.print(
            "[red]Error:[/red] Provide --search-url, --search-urls-file, or --commune."
        )
        raise typer.Exit(1)

    cfg = _make_settings_override(
        headless=headless,
        min_delay=min_delay,
        max_delay=max_delay,
        save_raw=save_raw,
        database_url=database_url,
        log_level=log_level,
    )
    if output_dir:
        cfg = cfg.model_copy(update={"output_dir": output_dir})

    asyncio.run(
        _scrape_all_async(
            search_urls=urls,
            max_pages=max_pages,
            max_listings=max_listings,
            headless=headless,
            min_delay=min_delay,
            max_delay=max_delay,
            save_raw=save_raw,
            settings=cfg,
        )
    )


async def _scrape_all_async(
    search_urls: list[str],
    max_pages: int = 10,
    max_listings: int = 200,
    headless: bool = True,
    min_delay: float = 2.5,
    max_delay: float = 6.0,
    save_raw: bool = True,
    settings: Optional[Settings] = None,
) -> None:
    from src.database import get_session_factory, init_db
    from src.models import ScraperRun
    from src.pipelines.scrape_details import run_details_pipeline
    from src.pipelines.scrape_search import run_search_pipeline
    from src.repositories.listing_repository import ListingRepository

    cfg = settings or get_settings()
    await init_db(cfg.database_url)

    run_id = _new_run_id()
    now = datetime.utcnow()

    scraper_run = ScraperRun(
        run_id=run_id,
        started_at=now,
        search_urls=search_urls,
        status="running",
    )

    # Persist the run record
    session_factory = get_session_factory(cfg.database_url)
    async with session_factory() as session:
        repo = ListingRepository(session)
        await repo.create_run(scraper_run)
        await repo.commit()

    console.print(f"[bold]Run ID:[/bold] {run_id}")
    console.print(f"[bold]Search URLs:[/bold] {len(search_urls)}")

    # Step 1: collect search cards
    console.print("[cyan]Step 1/3:[/cyan] Collecting listing URLs from search pages…")
    try:
        cards = await run_search_pipeline(
            search_urls=search_urls,
            run_id=run_id,
            settings=cfg,
            max_pages=max_pages,
            max_listings=max_listings,
        )
    except Exception as exc:
        logger.error("search_pipeline_error", error=str(exc))
        cards = []

    scraper_run.number_discovered = len(cards)
    console.print(f"  Discovered [green]{len(cards)}[/green] listings.")

    if not cards:
        console.print("[yellow]No listings found. Aborting.[/yellow]")
        scraper_run.status = "partial"
        scraper_run.finished_at = datetime.utcnow()
        async with session_factory() as session:
            repo = ListingRepository(session)
            await repo.update_run(scraper_run)
            await repo.commit()
        return

    # Step 2: fetch detail pages
    console.print("[cyan]Step 2/3:[/cyan] Fetching detail pages…")
    try:
        scraper_run = await run_details_pipeline(
            cards=cards,
            run_id=run_id,
            scraper_run=scraper_run,
            settings=cfg,
        )
        scraper_run.status = "completed"
    except Exception as exc:
        logger.error("details_pipeline_error", error=str(exc))
        scraper_run.status = "partial"
        scraper_run.error_message = str(exc)

    scraper_run.finished_at = datetime.utcnow()

    async with session_factory() as session:
        repo = ListingRepository(session)
        await repo.update_run(scraper_run)
        await repo.commit()

    # Step 3: export
    console.print("[cyan]Step 3/3:[/cyan] Exporting results…")
    await _export_run(run_id=run_id, settings=cfg, run_date=now.strftime("%Y-%m-%d"))

    # Summary table
    _print_run_summary(scraper_run)


async def _export_run(run_id: str, settings: Settings, run_date: str) -> None:
    from src.database import get_session_factory
    from src.exporters import export_csv, export_json, export_new_listings
    from src.repositories.listing_repository import ListingRepository

    session_factory = get_session_factory(settings.database_url)
    async with session_factory() as session:
        repo = ListingRepository(session)
        new_rows = list(await repo.get_by_run(run_id))
        all_active = list(await repo.get_all_active())

    date_dir = settings.output_dir / run_date
    date_dir.mkdir(parents=True, exist_ok=True)

    if all_active:
        export_csv(all_active, date_dir / "listings_all.csv")
        export_json(all_active, date_dir / "listings_all.json")

    if new_rows:
        # Get the full listing rows for newly discovered ones
        from src.database import get_async_engine, ListingRow
        from sqlalchemy import select

        engine = get_async_engine(settings.database_url)
        from sqlalchemy.ext.asyncio import AsyncSession
        session_factory2 = get_session_factory(settings.database_url)
        async with session_factory2() as session:
            # Get IDs from run snapshots
            run_listing_ids = [r.listing_id for r in new_rows]
            from sqlalchemy import select
            result = await session.execute(
                select(ListingRow).where(ListingRow.listing_id.in_(run_listing_ids))
            )
            run_listing_rows = result.scalars().all()

        if run_listing_rows:
            export_new_listings(run_listing_rows, settings.output_dir, run_date=run_date)

    console.print(f"  Exports written to [green]{settings.output_dir}[/green]")


def _print_run_summary(run) -> None:
    table = Table(title="Scraper Run Summary", show_header=False)
    table.add_column("Key", style="cyan")
    table.add_column("Value", style="white")
    table.add_row("Run ID", run.run_id)
    table.add_row("Status", run.status)
    table.add_row("Discovered", str(run.number_discovered))
    table.add_row("Scraped", str(run.number_scraped))
    table.add_row("New", str(run.number_new))
    table.add_row("Changed", str(run.number_changed))
    table.add_row("Failed", str(run.number_failed))
    console.print(table)


# ── export ────────────────────────────────────────────────────────────────────

@app.command("export")
def cmd_export(
    format: Optional[List[str]] = typer.Option(
        None, "--format", help="csv | json | parquet (repeat for multiple)"
    ),
    output_dir: Optional[Path] = typer.Option(None, "--output-dir"),
    database_url: Optional[str] = typer.Option(None, "--database-url"),
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """Export all active listings to CSV, JSON, and/or Parquet."""
    configure_logging(level=log_level)
    cfg = get_settings()
    url = database_url or cfg.database_url
    out_dir = output_dir or cfg.output_dir
    formats = format or ["csv", "json"]
    asyncio.run(_export_all_async(url, out_dir, formats))


async def _export_all_async(database_url: str, output_dir: Path, formats: list[str]) -> None:
    from src.database import get_session_factory, init_db
    from src.exporters import export_csv, export_json, export_parquet
    from src.repositories.listing_repository import ListingRepository

    await init_db(database_url)
    session_factory = get_session_factory(database_url)
    async with session_factory() as session:
        repo = ListingRepository(session)
        rows = list(await repo.get_all_active())

    if not rows:
        console.print("[yellow]No active listings in database.[/yellow]")
        return

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    output_dir.mkdir(parents=True, exist_ok=True)

    if "csv" in formats:
        p = export_csv(rows, output_dir / f"listings_{date_str}.csv")
        console.print(f"  CSV → [green]{p}[/green]")
    if "json" in formats:
        p = export_json(rows, output_dir / f"listings_{date_str}.json")
        console.print(f"  JSON → [green]{p}[/green]")
    if "parquet" in formats:
        p = export_parquet(rows, output_dir / f"listings_{date_str}.parquet")
        console.print(f"  Parquet → [green]{p}[/green]")

    console.print(f"[green]{len(rows)} listings exported.[/green]")


# ── detect-new ────────────────────────────────────────────────────────────────

@app.command("detect-new")
def cmd_detect_new(
    since: Optional[str] = typer.Option(
        None, "--since", help="ISO date string e.g. 2024-01-15. Defaults to last completed run."
    ),
    database_url: Optional[str] = typer.Option(None, "--database-url"),
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """Print and export listings first seen after the given date (or last run)."""
    configure_logging(level=log_level)
    cfg = get_settings()
    url = database_url or cfg.database_url
    asyncio.run(_detect_new_async(url, since, cfg.output_dir))


async def _detect_new_async(database_url: str, since_str: Optional[str], output_dir: Path) -> None:
    from src.database import get_session_factory, init_db
    from src.exporters import export_new_listings
    from src.repositories.listing_repository import ListingRepository

    await init_db(database_url)
    session_factory = get_session_factory(database_url)

    async with session_factory() as session:
        repo = ListingRepository(session)

        since_dt: datetime
        if since_str:
            since_dt = datetime.fromisoformat(since_str)
        else:
            last_run = await repo.get_last_run()
            if last_run and last_run.finished_at:
                since_dt = last_run.finished_at
            else:
                since_dt = datetime(2000, 1, 1)

        rows = list(await repo.get_new_since(since_dt))

    console.print(f"[green]{len(rows)} new listing(s) since {since_dt.date()}[/green]")

    if rows:
        paths = export_new_listings(rows, output_dir, run_date=datetime.utcnow().strftime("%Y-%m-%d"))
        for fmt, path in paths.items():
            console.print(f"  {fmt.upper()} → [green]{path}[/green]")

        # Print summary table
        table = Table("ID", "Title", "Price", "Commune", "First Seen")
        for r in rows[:20]:
            table.add_row(
                r.listing_id or "",
                (r.title or "")[:50],
                f"{r.currency} {r.price:,.0f}" if r.price else "",
                r.commune or "",
                r.first_seen_at.date().isoformat() if r.first_seen_at else "",
            )
        console.print(table)
        if len(rows) > 20:
            console.print(f"  … and {len(rows) - 20} more.")


# ── doctor ────────────────────────────────────────────────────────────────────

@app.command("doctor")
def cmd_doctor(
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """Check that all runtime dependencies are working correctly."""
    configure_logging(level=log_level)
    console.print("[bold]Running environment checks…[/bold]")

    ok = True

    # Python version
    import sys
    pyver = sys.version_info
    status = "✓" if pyver >= (3, 11) else "✗"
    if pyver < (3, 11):
        ok = False
    console.print(f"  Python {pyver.major}.{pyver.minor}: [{('green' if pyver >= (3,11) else 'red')}]{status}[/]")

    # Playwright
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.close()
            browser.close()
        console.print("  Playwright Chromium: [green]✓[/green]")
    except Exception as exc:
        console.print(f"  Playwright Chromium: [red]✗ {exc}[/red]")
        console.print("    Run: playwright install chromium --with-deps")
        ok = False

    # selectolax
    try:
        from selectolax.parser import HTMLParser
        HTMLParser("<html><body></body></html>")
        console.print("  selectolax: [green]✓[/green]")
    except Exception as exc:
        console.print(f"  selectolax: [red]✗ {exc}[/red]")
        ok = False

    # SQLite / aiosqlite
    try:
        import aiosqlite
        console.print("  aiosqlite: [green]✓[/green]")
    except ImportError:
        console.print("  aiosqlite: [red]✗ not installed[/red]")
        ok = False

    # Config files
    cfg = get_settings()
    from src.config import BASE_DIR
    for name in ("selectors.yaml", "defaults.yaml"):
        path = BASE_DIR / "configs" / name
        exists = path.exists()
        icon = "✓" if exists else "✗"
        color = "green" if exists else "red"
        console.print(f"  configs/{name}: [{color}]{icon}[/]")
        if not exists:
            ok = False

    # Database
    try:
        asyncio.run(_init_db_async(cfg.database_url))
        console.print(f"  Database ({cfg.database_url}): [green]✓[/green]")
    except Exception as exc:
        console.print(f"  Database: [red]✗ {exc}[/red]")
        ok = False

    console.print()
    if ok:
        console.print("[green bold]All checks passed. Ready to scrape![/green bold]")
    else:
        console.print("[red bold]Some checks failed. Fix the issues above before running.[/red bold]")
        raise typer.Exit(1)


# ── Utilities ─────────────────────────────────────────────────────────────────

def _resolve_search_urls(
    search_url: Optional[List[str]],
    search_urls_file: Optional[Path],
) -> list[str]:
    urls: list[str] = list(search_url or [])
    if search_urls_file:
        from src.utils import load_urls_from_file
        urls.extend(load_urls_from_file(search_urls_file))
    return urls


def _new_run_id() -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    short_id = str(uuid.uuid4())[:8]
    return f"{ts}_{short_id}"


# ── web ───────────────────────────────────────────────────────────────────────

@app.command("web")
def cmd_web(
    host: str = typer.Option("127.0.0.1", "--host", help="Bind host"),
    port: int = typer.Option(8080, "--port", help="Bind port"),
    reload: bool = typer.Option(False, "--reload/--no-reload", help="Auto-reload on code changes"),
) -> None:
    """Start the web UI (http://localhost:8080)."""
    import uvicorn
    console.print(f"[green]Starting web UI at[/green] http://{host}:{port}")
    uvicorn.run("src.web:app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    app()
