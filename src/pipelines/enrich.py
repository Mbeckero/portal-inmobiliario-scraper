"""Enrichment pipeline: post-processing and optional enhancements.

Currently implements:
- Image URL validation (HEAD request to check liveness)
- Optional geo-coordinate export for map visualization

Can be extended for:
- Reverse geocoding
- External price-index enrichment
"""
from __future__ import annotations

import asyncio
from typing import Optional

import httpx

from src.logging_config import get_logger

logger = get_logger(__name__)


async def validate_image_urls(
    image_urls: list[str],
    timeout: float = 5.0,
    max_concurrent: int = 10,
) -> list[str]:
    """Return only image URLs that respond with HTTP 200.

    Uses async HTTP HEAD requests; does not download images.
    """
    if not image_urls:
        return []

    semaphore = asyncio.Semaphore(max_concurrent)
    valid: list[str] = []

    async def check(url: str) -> Optional[str]:
        async with semaphore:
            try:
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                    r = await client.head(url)
                    if r.status_code == 200:
                        return url
            except Exception:
                pass
            return None

    results = await asyncio.gather(*[check(u) for u in image_urls])
    valid = [r for r in results if r is not None]
    logger.debug(
        "image_validation",
        total=len(image_urls),
        valid=len(valid),
    )
    return valid


def build_geo_export(listings: list[dict]) -> list[dict]:
    """Return a GeoJSON-friendly list of listings with lat/lng for mapping."""
    features = []
    for listing in listings:
        lat = listing.get("latitude")
        lng = listing.get("longitude")
        if lat is None or lng is None:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat],
                },
                "properties": {
                    "listing_id": listing.get("listing_id"),
                    "title": listing.get("title"),
                    "price": listing.get("price"),
                    "currency": listing.get("currency"),
                    "commune": listing.get("commune"),
                    "listing_url": listing.get("listing_url"),
                },
            }
        )
    return features


def export_geojson(listings: list[dict], output_path) -> None:
    import json

    output_path = str(output_path)
    collection = {"type": "FeatureCollection", "features": build_geo_export(listings)}
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(collection, fh, ensure_ascii=False, indent=2)
    logger.info("geojson_exported", path=output_path, features=len(collection["features"]))
