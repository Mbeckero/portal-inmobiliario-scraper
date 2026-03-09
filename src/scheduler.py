"""Daily scheduler for automated scraper runs.

Usage:
    python -m src.scheduler

This runs indefinitely, executing a full scrape at the configured daily hour.
For production use, prefer a cron job (see README).

Cron example (runs at 06:00 every day):
    0 6 * * * /path/to/.venv/bin/python -m src.cli scrape-all >> /path/to/data/logs/cron.log 2>&1
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

import schedule

from src.config import get_settings, load_defaults
from src.logging_config import configure_logging, get_logger

logger = get_logger(__name__)


def _get_default_search_urls() -> list[str]:
    """Return the default search URLs loaded from the .env or a urls file."""
    import os
    from pathlib import Path

    # Check for a urls file
    urls_file = Path("data/search_urls.txt")
    if urls_file.exists():
        from src.utils import load_urls_from_file
        return load_urls_from_file(urls_file)

    # Fallback: use the example URL from defaults
    return [
        "https://www.portalinmobiliario.com/venta/departamento/las-condes-metropolitana",
    ]


def _run_daily_job() -> None:
    """Synchronous wrapper for the async scrape-all job."""
    from src.cli import _scrape_all_async

    cfg = get_settings()
    configure_logging(level=cfg.log_level, log_file=cfg.log_dir / "scheduler.log")
    search_urls = _get_default_search_urls()

    logger.info("scheduled_run_start", search_urls=search_urls, time=datetime.utcnow().isoformat())

    try:
        asyncio.run(
            _scrape_all_async(
                search_urls=search_urls,
                max_pages=cfg.max_pages,
                max_listings=cfg.max_listings,
                headless=cfg.headless,
                min_delay=cfg.min_delay,
                max_delay=cfg.max_delay,
                save_raw=cfg.save_raw,
            )
        )
        logger.info("scheduled_run_complete")
    except Exception as exc:
        logger.error("scheduled_run_failed", error=str(exc))


def start_scheduler() -> None:
    defaults = load_defaults()
    sched_cfg = defaults.get("scheduler", {})
    run_hour = sched_cfg.get("daily_run_hour", 6)
    run_minute = sched_cfg.get("daily_run_minute", 0)

    run_at = f"{run_hour:02d}:{run_minute:02d}"
    logger.info("scheduler_start", run_at=run_at)

    schedule.every().day.at(run_at).do(_run_daily_job)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    cfg = get_settings()
    configure_logging(level=cfg.log_level, log_file=cfg.log_dir / "scheduler.log")
    start_scheduler()
