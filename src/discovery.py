"""URL generation and discovery helpers for Portal Inmobiliario.

The URL structure is:
  https://www.portalinmobiliario.com/{operation}/{property_type}/{location-slug}

Optional appended filters (URL suffix tokens):
  _Desde_{N}         — pagination offset (N = (page-1)*48 + 1, but PI uses 1-based)
  _OrderId_{SORT}    — sort order
  _PriceRange_{min}{UNIT}-{max}{UNIT}  — price range

Examples:
  /venta/departamento/las-condes-metropolitana
  /venta/departamento/las-condes-metropolitana/_Desde_49
  /venta/casa/santiago-metropolitana/_OrderId_PRICE
"""
from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse, urlunparse

from src.config import load_defaults
from src.logging_config import get_logger

logger = get_logger(__name__)

BASE_URL = "https://www.portalinmobiliario.com"


def build_search_url(
    operation: str = "sale",
    property_type: str = "apartment",
    location: str = "las-condes-metropolitana",
    page: int = 1,
    sort: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    price_unit: str = "CLF",  # CLF = UF; CLP for pesos
) -> str:
    """Build a Portal Inmobiliario search URL from parameters.

    Args:
        operation: "sale" or "rent" (will be mapped to "venta"/"arriendo")
        property_type: canonical type like "apartment", "house", etc.
        location: slug like "las-condes-metropolitana" or commune display name
        page: 1-indexed page number
        sort: optional sort key ("relevance", "price_asc", "price_desc", "newest")
        price_min: optional minimum price
        price_max: optional maximum price
        price_unit: "CLF" (UF) or "CLP"
    """
    defaults = load_defaults()["portal_inmobiliario"]

    # Map operation
    op_map = defaults["operations"]
    op_slug = op_map.get(operation, operation)

    # Map property type
    pt_map = defaults["property_types"]
    pt_slug = pt_map.get(property_type, property_type)

    # Resolve location slug
    location_slug = _resolve_location_slug(location, defaults)

    path = f"/{op_slug}/{pt_slug}/{location_slug}"

    # Collect filter tokens
    tokens: list[str] = []

    # Pagination
    items_per_page = defaults.get("items_per_page", 48)
    if page > 1:
        offset = (page - 1) * items_per_page + 1
        tokens.append(f"_Desde_{offset}")

    # Sort
    sort_map = defaults.get("sort_options", {})
    if sort and sort in sort_map:
        tokens.append(f"_OrderId_{sort_map[sort]}")

    # Price range
    if price_min is not None or price_max is not None:
        lo = f"{int(price_min)}{price_unit}" if price_min is not None else f"0{price_unit}"
        hi = f"{int(price_max)}{price_unit}" if price_max is not None else f"999999999{price_unit}"
        tokens.append(f"_PriceRange_{lo}-{hi}")

    if tokens:
        path += "/" + "".join(tokens)

    return BASE_URL + path


def _resolve_location_slug(location: str, defaults: dict) -> str:
    """Convert a commune display name or raw slug to a URL slug."""
    communes = defaults.get("communes", {})
    # Direct match by display name
    if location in communes:
        return communes[location]
    # If already looks like a slug, use as-is
    if re.match(r"^[a-z0-9-]+$", location):
        return location
    # Build slug from name
    from src.utils import slugify
    slug = slugify(location)
    # Append "-metropolitana" if it's a bare commune name without a region suffix
    if "-" not in slug or not any(
        slug.endswith(r) for r in (
            "metropolitana", "valparaiso", "biobio", "araucania",
            "los-lagos", "coquimbo", "antofagasta", "maule",
        )
    ):
        slug += "-metropolitana"
    return slug


def paginate_url(base_url: str, page: int, items_per_page: int = 48) -> str:
    """Return a paginated version of an existing search URL.

    If the URL already has a _Desde_ token, replace it.
    """
    url = base_url.rstrip("/")
    # Remove existing _Desde_ token
    url = re.sub(r"/_Desde_\d+", "", url)
    if page == 1:
        return url
    offset = (page - 1) * items_per_page + 1
    return url + f"/_Desde_{offset}"


def extract_page_number(url: str) -> int:
    """Extract current page number from a Portal Inmobiliario search URL."""
    m = re.search(r"_Desde_(\d+)", url)
    if m:
        offset = int(m.group(1))
        items_per_page = load_defaults()["portal_inmobiliario"].get("items_per_page", 48)
        return (offset - 1) // items_per_page + 1
    return 1


def generate_search_urls_for_commune(
    commune: str,
    operation: str = "sale",
    property_types: Optional[list[str]] = None,
) -> list[str]:
    """Generate one search URL per property type for a given commune."""
    if property_types is None:
        property_types = ["apartment", "house"]
    return [
        build_search_url(
            operation=operation,
            property_type=pt,
            location=commune,
        )
        for pt in property_types
    ]
