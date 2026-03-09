"""Deduplication and change-detection logic."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from src.logging_config import get_logger
from src.models import Listing, ListingChange

logger = get_logger(__name__)

# Fields that, when changed, produce a ListingChange record
TRACKED_FIELDS = (
    "price",
    "price_clp",
    "price_uf",
    "is_active",
    "maintenance_fee",
    "bedrooms",
    "bathrooms",
    "usable_area_m2",
    "title",
)


def compute_fingerprint(listing: Listing) -> str:
    """Compute (or recompute) the listing fingerprint and attach it."""
    from src.normalization import generate_listing_fingerprint

    fp = generate_listing_fingerprint(
        listing_id=listing.listing_id,
        price=listing.price,
        bedrooms=listing.bedrooms,
        usable_area_m2=listing.usable_area_m2,
        commune=listing.commune,
    )
    listing.fingerprint = fp
    return fp


def is_duplicate(new: Listing, existing: Listing) -> bool:
    """Return True if two listings represent the same property at the same price."""
    if new.listing_id == existing.listing_id:
        return True
    if new.fingerprint and existing.fingerprint:
        return new.fingerprint == existing.fingerprint
    return False


def detect_changes(
    new: Listing,
    existing: Listing,
    run_id: str | None = None,
) -> list[ListingChange]:
    """Compare tracked fields between a freshly-scraped and stored listing.

    Returns a list of ListingChange records (empty if nothing changed).
    """
    changes: list[ListingChange] = []
    now = datetime.utcnow()

    for field in TRACKED_FIELDS:
        old_val: Any = getattr(existing, field, None)
        new_val: Any = getattr(new, field, None)
        if _values_differ(old_val, new_val):
            changes.append(
                ListingChange(
                    listing_id=new.listing_id,
                    listing_url=new.listing_url,
                    field_name=field,
                    old_value=old_val,
                    new_value=new_val,
                    detected_at=now,
                    run_id=run_id,
                )
            )
            logger.info(
                "change_detected",
                listing_id=new.listing_id,
                field=field,
                old=old_val,
                new=new_val,
            )

    return changes


def _values_differ(a: Any, b: Any) -> bool:
    """Tolerant comparison; treats None == None as equal."""
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    if isinstance(a, float) and isinstance(b, float):
        return abs(a - b) > 0.01
    return a != b


def merge_listing(new: Listing, existing: Listing) -> Listing:
    """Produce an updated listing by merging new data on top of existing,
    preserving first_seen_at and other historical fields.
    """
    merged = new.model_copy(deep=True)
    merged.first_seen_at = existing.first_seen_at or existing.scraped_at
    merged.last_seen_at = new.scraped_at
    merged.is_active = True
    return merged
