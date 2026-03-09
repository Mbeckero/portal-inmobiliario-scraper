"""FastAPI web UI for Portal Inmobiliario Scraper."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import desc, func, select

BASE_DIR = Path(__file__).parent.parent
UI_DIR = BASE_DIR / "ui"

app = FastAPI(title="Portal Inmobiliario Scraper", docs_url=None, redoc_url=None)

_current_task: Optional[asyncio.Task] = None


# ── UI ────────────────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    return FileResponse(UI_DIR / "index.html")


# ── Defaults & settings ───────────────────────────────────────────────────────

@app.get("/api/defaults")
async def get_defaults():
    from src.config import load_defaults
    d = load_defaults()["portal_inmobiliario"]
    return {
        "communes": list(d["communes"].keys()),
        "property_types": list(d["property_types"].keys()),
        "operations": list(d["operations"].keys()),
        "sort_options": list(d["sort_options"].keys()),
    }


@app.get("/api/settings")
async def get_settings_api():
    from src.config import get_settings
    cfg = get_settings()
    return {
        "headless": cfg.headless,
        "browser_timeout": cfg.browser_timeout,
        "min_delay": cfg.min_delay,
        "max_delay": cfg.max_delay,
        "max_concurrency": cfg.max_concurrency,
        "max_pages": cfg.max_pages,
        "max_listings": cfg.max_listings,
        "save_raw": cfg.save_raw,
        "max_retries": cfg.max_retries,
        "retry_backoff": cfg.retry_backoff,
        "log_level": cfg.log_level,
        "database_url": cfg.database_url,
        "output_dir": str(cfg.output_dir),
    }


class SettingsUpdate(BaseModel):
    headless: Optional[bool] = None
    browser_timeout: Optional[int] = None
    min_delay: Optional[float] = None
    max_delay: Optional[float] = None
    max_concurrency: Optional[int] = None
    max_pages: Optional[int] = None
    max_listings: Optional[int] = None
    save_raw: Optional[bool] = None
    max_retries: Optional[int] = None
    retry_backoff: Optional[float] = None
    log_level: Optional[str] = None
    database_url: Optional[str] = None
    output_dir: Optional[str] = None


@app.post("/api/settings")
async def update_settings(body: SettingsUpdate):
    env_path = BASE_DIR / ".env"
    existing: dict[str, str] = {}
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                existing[k.strip().upper()] = v.strip()

    mapping = {
        "headless": ("HEADLESS", lambda v: str(v).lower()),
        "browser_timeout": ("BROWSER_TIMEOUT", str),
        "min_delay": ("MIN_DELAY", str),
        "max_delay": ("MAX_DELAY", str),
        "max_concurrency": ("MAX_CONCURRENCY", str),
        "max_pages": ("MAX_PAGES", str),
        "max_listings": ("MAX_LISTINGS", str),
        "save_raw": ("SAVE_RAW", lambda v: str(v).lower()),
        "max_retries": ("MAX_RETRIES", str),
        "retry_backoff": ("RETRY_BACKOFF", str),
        "log_level": ("LOG_LEVEL", str),
        "database_url": ("DATABASE_URL", str),
        "output_dir": ("OUTPUT_DIR", str),
    }

    for field, value in body.model_dump(exclude_none=True).items():
        if field in mapping:
            key, formatter = mapping[field]
            existing[key] = formatter(value)

    env_path.write_text("\n".join(f"{k}={v}" for k, v in existing.items()) + "\n")

    from src.config import get_settings
    get_settings.cache_clear()

    return {"ok": True}


# ── Runs ──────────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    search_url: Optional[str] = None
    operation_type: str = "sale"
    property_type: str = "apartment"
    commune: Optional[str] = None
    sort: Optional[str] = None
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    price_unit: str = "CLF"
    max_pages: int = 10
    max_listings: int = 200
    headless: bool = True
    min_delay: float = 2.5
    max_delay: float = 6.0
    save_raw: bool = False


@app.post("/api/runs")
async def start_run(body: ScrapeRequest):
    global _current_task

    if _current_task and not _current_task.done():
        raise HTTPException(status_code=409, detail="A run is already in progress")

    if body.search_url:
        urls = [body.search_url]
    elif body.commune:
        from src.discovery import build_search_url
        urls = [build_search_url(
            operation=body.operation_type,
            property_type=body.property_type,
            location=body.commune,
            sort=body.sort or None,
            price_min=body.price_min,
            price_max=body.price_max,
            price_unit=body.price_unit,
        )]
    else:
        raise HTTPException(status_code=400, detail="Provide search_url or commune")

    from src.cli import _scrape_all_async
    from src.config import get_settings

    cfg = get_settings().model_copy(update={
        "headless": body.headless,
        "min_delay": body.min_delay,
        "max_delay": body.max_delay,
        "save_raw": body.save_raw,
        "max_pages": body.max_pages,
        "max_listings": body.max_listings,
    })

    _current_task = asyncio.create_task(
        _scrape_all_async(
            search_urls=urls,
            max_pages=body.max_pages,
            max_listings=body.max_listings,
            headless=body.headless,
            min_delay=body.min_delay,
            max_delay=body.max_delay,
            save_raw=body.save_raw,
            settings=cfg,
        )
    )

    return {"status": "started", "search_urls": urls}


@app.get("/api/status")
async def get_status():
    running = _current_task is not None and not _current_task.done()
    error = None
    if _current_task and _current_task.done():
        exc = _current_task.exception()
        if exc:
            error = str(exc)
    return {"running": running, "error": error}


@app.get("/api/runs")
async def list_runs():
    from src.config import get_settings
    from src.database import ScraperRunRow, get_session_factory, init_db

    cfg = get_settings()
    await init_db(cfg.database_url)
    session_factory = get_session_factory(cfg.database_url)

    async with session_factory() as session:
        result = await session.execute(
            select(ScraperRunRow).order_by(desc(ScraperRunRow.started_at)).limit(50)
        )
        rows = result.scalars().all()

    running = _current_task is not None and not _current_task.done()

    return [
        {
            "run_id": r.run_id,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            "status": "running" if (running and i == 0) else r.status,
            "number_discovered": r.number_discovered,
            "number_scraped": r.number_scraped,
            "number_new": r.number_new,
            "number_changed": r.number_changed,
            "number_failed": r.number_failed,
            "search_urls": json.loads(r.search_urls) if r.search_urls else [],
            "error_message": r.error_message,
        }
        for i, r in enumerate(rows)
    ]


@app.get("/api/runs/{run_id}")
async def get_run(run_id: str):
    from src.config import get_settings
    from src.database import ScraperRunRow, get_session_factory, init_db

    cfg = get_settings()
    session_factory = get_session_factory(cfg.database_url)

    async with session_factory() as session:
        result = await session.execute(
            select(ScraperRunRow).where(ScraperRunRow.run_id == run_id)
        )
        row = result.scalar_one_or_none()

    if not row:
        raise HTTPException(status_code=404, detail="Run not found")

    running = _current_task is not None and not _current_task.done()

    return {
        "run_id": row.run_id,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "status": "running" if running else row.status,
        "number_discovered": row.number_discovered,
        "number_scraped": row.number_scraped,
        "number_new": row.number_new,
        "number_changed": row.number_changed,
        "number_failed": row.number_failed,
        "search_urls": json.loads(row.search_urls) if row.search_urls else [],
        "error_message": row.error_message,
    }


# ── Export ────────────────────────────────────────────────────────────────────

class ExportRequest(BaseModel):
    formats: list[str] = ["csv", "json"]


@app.post("/api/export")
async def trigger_export(body: ExportRequest):
    from src.config import get_settings
    from src.database import get_session_factory, init_db
    from src.exporters import export_csv, export_json, export_parquet
    from src.repositories.listing_repository import ListingRepository

    cfg = get_settings()
    await init_db(cfg.database_url)
    session_factory = get_session_factory(cfg.database_url)

    async with session_factory() as session:
        repo = ListingRepository(session)
        rows = list(await repo.get_all_active())

    if not rows:
        return {"ok": False, "message": "No active listings in database", "count": 0}

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = cfg.output_dir / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    exported = {}
    if "csv" in body.formats:
        p = export_csv(rows, out_dir / f"listings_{date_str}.csv")
        exported["csv"] = str(p)
    if "json" in body.formats:
        p = export_json(rows, out_dir / f"listings_{date_str}.json")
        exported["json"] = str(p)
    if "parquet" in body.formats:
        p = export_parquet(rows, out_dir / f"listings_{date_str}.parquet")
        exported["parquet"] = str(p)

    return {"ok": True, "exported": exported, "count": len(rows)}


# ── Listings ──────────────────────────────────────────────────────────────────

@app.get("/api/listings")
async def list_listings(limit: int = 50, offset: int = 0):
    from src.config import get_settings
    from src.database import ListingRow, get_session_factory, init_db, row_to_dict

    cfg = get_settings()
    await init_db(cfg.database_url)
    session_factory = get_session_factory(cfg.database_url)

    async with session_factory() as session:
        total_result = await session.execute(
            select(func.count()).select_from(ListingRow).where(ListingRow.is_active == True)
        )
        total = total_result.scalar_one()

        result = await session.execute(
            select(ListingRow)
            .where(ListingRow.is_active == True)
            .order_by(desc(ListingRow.scraped_at))
            .offset(offset)
            .limit(limit)
        )
        rows = result.scalars().all()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [row_to_dict(r) for r in rows],
    }


# ── Entrypoint ────────────────────────────────────────────────────────────────

def run(host: str = "127.0.0.1", port: int = 8080, reload: bool = False):
    import uvicorn
    uvicorn.run("src.web:app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    run()
