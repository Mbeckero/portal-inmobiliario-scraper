"""SQLAlchemy ORM models and async database operations."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional, Sequence

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
    select,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.logging_config import get_logger

logger = get_logger(__name__)


# ── ORM base ──────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


# ── ORM tables ────────────────────────────────────────────────────────────────

class ListingRow(Base):
    """Current state of each listing (upserted on each scrape)."""

    __tablename__ = "listings"

    listing_id: Mapped[str] = mapped_column(String, primary_key=True)
    source_site: Mapped[str] = mapped_column(String, default="portalinmobiliario.com")
    listing_url: Mapped[str] = mapped_column(String, nullable=False)
    canonical_url: Mapped[Optional[str]] = mapped_column(String)
    search_url: Mapped[Optional[str]] = mapped_column(String)
    title: Mapped[Optional[str]] = mapped_column(String)
    publication_type: Mapped[str] = mapped_column(String, default="sale")
    property_type: Mapped[Optional[str]] = mapped_column(String)
    region: Mapped[Optional[str]] = mapped_column(String)
    commune: Mapped[Optional[str]] = mapped_column(String)
    neighborhood: Mapped[Optional[str]] = mapped_column(String)
    full_location_text: Mapped[Optional[str]] = mapped_column(String)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    currency: Mapped[Optional[str]] = mapped_column(String(10))
    price: Mapped[Optional[float]] = mapped_column(Float)
    price_clp: Mapped[Optional[float]] = mapped_column(Float)
    price_uf: Mapped[Optional[float]] = mapped_column(Float)
    price_is_from: Mapped[bool] = mapped_column(Boolean, default=False)
    maintenance_fee: Mapped[Optional[float]] = mapped_column(Float)
    maintenance_fee_currency: Mapped[Optional[str]] = mapped_column(String(10))
    bedrooms: Mapped[Optional[int]] = mapped_column(Integer)
    bathrooms: Mapped[Optional[int]] = mapped_column(Integer)
    parking_spaces: Mapped[Optional[int]] = mapped_column(Integer)
    storage_room: Mapped[Optional[bool]] = mapped_column(Boolean)
    usable_area_m2: Mapped[Optional[float]] = mapped_column(Float)
    total_area_m2: Mapped[Optional[float]] = mapped_column(Float)
    land_area_m2: Mapped[Optional[float]] = mapped_column(Float)
    year_built: Mapped[Optional[int]] = mapped_column(Integer)
    orientation: Mapped[Optional[str]] = mapped_column(String)
    furnished: Mapped[Optional[bool]] = mapped_column(Boolean)
    condition: Mapped[Optional[str]] = mapped_column(String)
    seller_name: Mapped[Optional[str]] = mapped_column(String)
    seller_type: Mapped[str] = mapped_column(String, default="unknown")
    agency_name: Mapped[Optional[str]] = mapped_column(String)
    description: Mapped[Optional[str]] = mapped_column(Text)
    features: Mapped[Optional[str]] = mapped_column(Text)      # JSON array
    image_urls: Mapped[Optional[str]] = mapped_column(Text)    # JSON array
    main_image_url: Mapped[Optional[str]] = mapped_column(String)
    listing_position: Mapped[Optional[int]] = mapped_column(Integer)
    scraped_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    first_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    raw_html_path: Mapped[Optional[str]] = mapped_column(String)
    raw_payload_path: Mapped[Optional[str]] = mapped_column(String)
    fingerprint: Mapped[Optional[str]] = mapped_column(String(64))


class ListingRunRow(Base):
    """One row per (listing_id, run_id) snapshot — historical tracking."""

    __tablename__ = "listing_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    listing_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    scraped_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    price: Mapped[Optional[float]] = mapped_column(Float)
    currency: Mapped[Optional[str]] = mapped_column(String(10))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    fingerprint: Mapped[Optional[str]] = mapped_column(String(64))
    change_summary: Mapped[Optional[str]] = mapped_column(Text)   # JSON list of changed fields


class ScraperRunRow(Base):
    """One row per scraper execution."""

    __tablename__ = "scraper_runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String, default="running")
    search_urls: Mapped[Optional[str]] = mapped_column(Text)    # JSON array
    number_discovered: Mapped[int] = mapped_column(Integer, default=0)
    number_scraped: Mapped[int] = mapped_column(Integer, default=0)
    number_new: Mapped[int] = mapped_column(Integer, default=0)
    number_changed: Mapped[int] = mapped_column(Integer, default=0)
    number_failed: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)


# ── Engine factory ────────────────────────────────────────────────────────────

_async_engine = None
_async_session_factory = None


def get_async_engine(database_url: str):
    global _async_engine
    if _async_engine is None:
        _async_engine = create_async_engine(database_url, echo=False)
    return _async_engine


def get_session_factory(database_url: str) -> async_sessionmaker[AsyncSession]:
    global _async_session_factory
    if _async_session_factory is None:
        engine = get_async_engine(database_url)
        _async_session_factory = async_sessionmaker(engine, expire_on_commit=False)
    return _async_session_factory


async def init_db(database_url: str) -> None:
    """Create all tables if they don't exist."""
    engine = get_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("database_initialized", url=database_url)


# ── Row ↔ model converters ────────────────────────────────────────────────────

def listing_to_row(listing: "src.models.Listing") -> dict[str, Any]:  # type: ignore[name-defined]
    d = listing.model_dump(exclude={"raw_json"})
    d["features"] = json.dumps(d.get("features") or [], ensure_ascii=False)
    d["image_urls"] = json.dumps(d.get("image_urls") or [], ensure_ascii=False)
    if isinstance(d.get("publication_type"), str):
        pass  # already a string
    else:
        d["publication_type"] = (d.get("publication_type") or "sale")
    if isinstance(d.get("property_type"), str):
        pass
    else:
        d["property_type"] = d.get("property_type")
    if isinstance(d.get("seller_type"), str):
        pass
    else:
        d["seller_type"] = "unknown"
    return d


def row_to_dict(row: ListingRow) -> dict[str, Any]:
    d = {c.name: getattr(row, c.name) for c in row.__table__.columns}
    d["features"] = json.loads(d.get("features") or "[]")
    d["image_urls"] = json.loads(d.get("image_urls") or "[]")
    return d
