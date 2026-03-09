"""Pydantic data models for the scraper.

All persistence and inter-module contracts go through these models.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


class OperationType(str, Enum):
    SALE = "sale"
    RENT = "rent"


class PropertyType(str, Enum):
    APARTMENT = "apartment"
    HOUSE = "house"
    LAND = "land"
    OFFICE = "office"
    COMMERCIAL = "commercial"
    WAREHOUSE = "warehouse"
    PARKING = "parking"
    OTHER = "other"


class SellerType(str, Enum):
    AGENCY = "agency"
    OWNER = "owner"
    DEVELOPER = "developer"
    UNKNOWN = "unknown"


class Listing(BaseModel):
    """Fully normalized real-estate listing."""

    # ── Identity ──────────────────────────────────────────────────────────
    listing_id: str
    source_site: str = "portalinmobiliario.com"
    search_url: Optional[str] = None
    listing_url: str
    canonical_url: Optional[str] = None

    # ── Classification ────────────────────────────────────────────────────
    title: Optional[str] = None
    publication_type: OperationType = OperationType.SALE
    property_type: Optional[PropertyType] = None

    # ── Location ──────────────────────────────────────────────────────────
    region: Optional[str] = None
    commune: Optional[str] = None
    neighborhood: Optional[str] = None
    full_location_text: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # ── Price ─────────────────────────────────────────────────────────────
    currency: Optional[str] = None          # "CLP" | "UF" | "USD"
    price: Optional[float] = None           # raw numeric, in stated currency
    price_clp: Optional[float] = None
    price_uf: Optional[float] = None
    price_is_from: bool = False             # True when listing says "Desde"
    maintenance_fee: Optional[float] = None
    maintenance_fee_currency: Optional[str] = None

    # ── Physical attributes ───────────────────────────────────────────────
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    parking_spaces: Optional[int] = None
    storage_room: Optional[bool] = None
    usable_area_m2: Optional[float] = None
    total_area_m2: Optional[float] = None
    land_area_m2: Optional[float] = None
    year_built: Optional[int] = None
    orientation: Optional[str] = None
    furnished: Optional[bool] = None
    condition: Optional[str] = None         # "new" | "used" | "project"

    # ── Seller ────────────────────────────────────────────────────────────
    seller_name: Optional[str] = None
    seller_type: SellerType = SellerType.UNKNOWN
    agency_name: Optional[str] = None

    # ── Content ───────────────────────────────────────────────────────────
    description: Optional[str] = None
    features: list[str] = Field(default_factory=list)
    image_urls: list[str] = Field(default_factory=list)
    main_image_url: Optional[str] = None

    # ── Metadata ─────────────────────────────────────────────────────────
    listing_position: Optional[int] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    published_at: Optional[datetime] = None
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    is_active: bool = True

    # ── Debug / raw storage ───────────────────────────────────────────────
    raw_html_path: Optional[str] = None
    raw_payload_path: Optional[str] = None
    raw_json: Optional[dict[str, Any]] = None

    # ── Deduplication ─────────────────────────────────────────────────────
    fingerprint: Optional[str] = None

    @model_validator(mode="after")
    def _set_main_image(self) -> "Listing":
        if self.image_urls and self.main_image_url is None:
            self.main_image_url = self.image_urls[0]
        return self


class SearchCard(BaseModel):
    """Lightweight data extracted from a search-result card.

    A SearchCard is enriched into a full Listing after visiting the detail page.
    """

    listing_id: str
    listing_url: str
    title: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    location_text: Optional[str] = None
    attributes_raw: list[str] = Field(default_factory=list)
    listing_position: Optional[int] = None
    search_url: Optional[str] = None
    thumbnail_url: Optional[str] = None


class ScraperRun(BaseModel):
    """Metadata for a single scraper execution."""

    run_id: str
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    status: str = "running"         # running | completed | failed | partial
    search_urls: list[str] = Field(default_factory=list)
    number_discovered: int = 0
    number_scraped: int = 0
    number_new: int = 0
    number_changed: int = 0
    number_failed: int = 0
    error_message: Optional[str] = None


class ListingChange(BaseModel):
    """Represents a detected change between two scrapes of the same listing."""

    listing_id: str
    listing_url: str
    field_name: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    run_id: Optional[str] = None
