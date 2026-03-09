"""Parse a Portal Inmobiliario listing detail (VIP) page.

Extraction strategy (in priority order):
  1. window.__INITIAL_STATE__ / window-level JSON state
  2. JSON-LD structured data
  3. OpenGraph / meta tags
  4. CSS selectors from configs/selectors.yaml
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

from selectolax.parser import HTMLParser, Node

from src.config import load_selectors
from src.logging_config import get_logger
from src.models import Listing, OperationType, PropertyType, SellerType
from src.normalization import (
    normalize_condition,
    normalize_property_type,
    normalize_region,
    normalize_text,
    parse_area_m2,
    parse_bool,
    parse_expenses,
    parse_location_components,
    parse_price,
    parse_room_attributes,
)
from src.parsers.structured_data_parser import (
    extract_coordinates_from_state,
    extract_json_ld,
    extract_listing_id_from_state,
    extract_meta_tags,
    extract_window_state,
)
from src.utils import extract_listing_id, parse_spanish_date, url_to_id

logger = get_logger(__name__)


def _sel(key: str) -> list[str]:
    selectors = load_selectors()
    value = selectors.get("detail", {}).get(key, [])
    if isinstance(value, str):
        return [value]
    return list(value)


def _first(tree: HTMLParser, selectors: list[str]) -> Optional[Node]:
    for sel in selectors:
        try:
            n = tree.css_first(sel)
            if n is not None:
                return n
        except Exception:
            continue
    return None


def _all(tree: HTMLParser, selectors: list[str]) -> list[Node]:
    for sel in selectors:
        try:
            nodes = tree.css(sel)
            if nodes:
                return nodes
        except Exception:
            continue
    return []


def _text(node: Optional[Node], strip: bool = True) -> Optional[str]:
    if node is None:
        return None
    raw = node.text(strip=strip)
    return normalize_text(raw)


def parse_detail_page(
    html: str,
    listing_url: str,
    search_url: Optional[str] = None,
    listing_position: Optional[int] = None,
    run_id: Optional[str] = None,
    raw_html_path: Optional[str] = None,
) -> Optional[Listing]:
    """Parse a detail page and return a fully populated Listing, or None on hard failure."""

    try:
        return _parse_detail_page(
            html=html,
            listing_url=listing_url,
            search_url=search_url,
            listing_position=listing_position,
            run_id=run_id,
            raw_html_path=raw_html_path,
        )
    except Exception as exc:
        logger.error("detail_parse_error", url=listing_url, error=str(exc))
        return None


def _parse_detail_page(
    html: str,
    listing_url: str,
    search_url: Optional[str],
    listing_position: Optional[int],
    run_id: Optional[str],
    raw_html_path: Optional[str],
) -> Optional[Listing]:
    tree = HTMLParser(html)
    now = datetime.utcnow()

    # ── Layer 1: window state ────────────────────────────────────────────────
    state = extract_window_state(html)
    raw_json: Optional[dict] = state

    # ── Layer 2: JSON-LD ─────────────────────────────────────────────────────
    json_lds = extract_json_ld(html)
    real_estate_ld: Optional[dict[str, Any]] = None
    for ld in json_lds:
        if ld.get("@type") in ("RealEstateListing", "Apartment", "House", "Product"):
            real_estate_ld = ld
            break

    # ── Layer 3: meta tags ───────────────────────────────────────────────────
    meta = extract_meta_tags(html)

    # ── Determine listing ID ─────────────────────────────────────────────────
    listing_id = (
        extract_listing_id(listing_url)
        or (state and extract_listing_id_from_state(state))
        or url_to_id(listing_url)
    )

    # ── Title ────────────────────────────────────────────────────────────────
    title = (
        _text(_first(tree, _sel("title")))
        or (real_estate_ld and real_estate_ld.get("name"))
        or meta.get("og:title")
        or meta.get("title")
    )
    title = normalize_text(title)

    # ── Price ────────────────────────────────────────────────────────────────
    price_node = _first(tree, _sel("price_container"))
    price_val: Optional[float] = None
    currency: Optional[str] = None
    price_is_from = False

    if price_node:
        raw_price = price_node.text(strip=True)
        price_val, currency, price_is_from = parse_price(raw_price)

    if price_val is None:
        # Try fraction + symbol separately
        frac = _first(tree, _sel("price_fraction"))
        sym = _first(tree, _sel("price_symbol"))
        raw_combined = ""
        if sym:
            raw_combined += _text(sym) or ""
        if frac:
            raw_combined += " " + (_text(frac) or "")
        if raw_combined.strip():
            price_val, currency, price_is_from = parse_price(raw_combined)

    # Classify as UF vs CLP
    price_clp: Optional[float] = None
    price_uf: Optional[float] = None
    if price_val is not None:
        if currency == "UF":
            price_uf = price_val
        elif currency == "CLP":
            price_clp = price_val

    # ── Maintenance fee ──────────────────────────────────────────────────────
    maint_node = _first(tree, _sel("maintenance_fee_row"))
    maint_fee: Optional[float] = None
    maint_currency: Optional[str] = None
    if maint_node:
        maint_fee, maint_currency = parse_expenses(maint_node.text(strip=True))

    # ── Highlighted specs (area, bedrooms, bathrooms, parking) ───────────────
    spec_nodes = _all(tree, _sel("highlighted_specs"))
    spec_texts = [normalize_text(n.text(strip=True)) for n in spec_nodes if n.text(strip=True)]
    spec_texts = [s for s in spec_texts if s]

    rooms = parse_room_attributes(spec_texts)
    bedrooms: Optional[int] = rooms.get("bedrooms")  # type: ignore[assignment]
    bathrooms: Optional[int] = rooms.get("bathrooms")  # type: ignore[assignment]
    parking_spaces: Optional[int] = rooms.get("parking_spaces")  # type: ignore[assignment]
    storage_room: Optional[bool] = rooms.get("storage_room")  # type: ignore[assignment]

    # Area: search spec texts for something with m²
    usable_area_m2: Optional[float] = None
    total_area_m2: Optional[float] = None
    land_area_m2: Optional[float] = None
    for spec in spec_texts:
        if any(u in spec.lower() for u in ("m²", "m2", "mt2", "mts", "ha")):
            val = parse_area_m2(spec)
            if val is None:
                continue
            low = spec.lower()
            if "terreno" in low or "sitio" in low:
                land_area_m2 = val
            elif "total" in low:
                total_area_m2 = val
            elif usable_area_m2 is None:
                usable_area_m2 = val

    # ── Full attributes table ────────────────────────────────────────────────
    table_rows = _all(tree, _sel("attributes_table"))
    extra_attrs = _parse_attributes_table(table_rows, tree)
    if bedrooms is None:
        bedrooms = extra_attrs.get("bedrooms")
    if bathrooms is None:
        bathrooms = extra_attrs.get("bathrooms")
    if parking_spaces is None:
        parking_spaces = extra_attrs.get("parking_spaces")
    if storage_room is None:
        storage_room = extra_attrs.get("storage_room")
    if usable_area_m2 is None:
        usable_area_m2 = extra_attrs.get("usable_area_m2")
    if total_area_m2 is None:
        total_area_m2 = extra_attrs.get("total_area_m2")
    if land_area_m2 is None:
        land_area_m2 = extra_attrs.get("land_area_m2")

    year_built: Optional[int] = extra_attrs.get("year_built")
    orientation: Optional[str] = extra_attrs.get("orientation")
    furnished: Optional[bool] = extra_attrs.get("furnished")
    condition: Optional[str] = extra_attrs.get("condition") or normalize_condition(
        _text(_first(tree, _sel("subtitle")))
    )
    features: list[str] = extra_attrs.get("features", [])

    # ── Location ─────────────────────────────────────────────────────────────
    loc_node = _first(tree, _sel("location_address"))
    full_location_text = (
        _text(loc_node)
        or (real_estate_ld and _ld_location(real_estate_ld))
        or meta.get("og:locality")
    )
    full_location_text = normalize_text(full_location_text)

    region, commune, neighborhood = parse_location_components(full_location_text)

    # Coordinates
    lat: Optional[float] = None
    lng: Optional[float] = None
    if state:
        lat, lng = extract_coordinates_from_state(state)
    if lat is None and real_estate_ld:
        geo = real_estate_ld.get("geo", {})
        try:
            lat = float(geo.get("latitude", 0)) or None
            lng = float(geo.get("longitude", 0)) or None
        except (TypeError, ValueError):
            pass

    # ── Description ──────────────────────────────────────────────────────────
    desc_nodes = _all(tree, _sel("description"))
    description: Optional[str] = None
    if desc_nodes:
        description = normalize_text(" ".join(n.text(strip=True) for n in desc_nodes))
    if not description and real_estate_ld:
        description = real_estate_ld.get("description")

    # ── Images ───────────────────────────────────────────────────────────────
    image_urls = _extract_images(tree)
    if not image_urls and real_estate_ld:
        imgs = real_estate_ld.get("image", [])
        if isinstance(imgs, str):
            image_urls = [imgs]
        elif isinstance(imgs, list):
            image_urls = [i for i in imgs if isinstance(i, str)]

    # ── Seller ───────────────────────────────────────────────────────────────
    seller_name = normalize_text(_text(_first(tree, _sel("seller_name"))))
    agency_name = normalize_text(_text(_first(tree, _sel("agency_name"))))
    seller_type_raw = _text(_first(tree, _sel("seller_badge")))
    seller_type = _classify_seller(seller_type_raw, seller_name, agency_name)

    # ── Published date ───────────────────────────────────────────────────────
    published_at: Optional[datetime] = None
    date_node = _first(tree, _sel("published_date"))
    if date_node:
        datetime_attr = date_node.attributes.get("datetime")
        if datetime_attr:
            published_at = parse_spanish_date(datetime_attr)
        else:
            published_at = parse_spanish_date(_text(date_node) or "")

    # ── Property / operation type ─────────────────────────────────────────────
    subtitle = normalize_text(_text(_first(tree, _sel("subtitle"))))
    prop_type_str = normalize_property_type(subtitle or title)
    property_type: Optional[PropertyType] = None
    if prop_type_str:
        try:
            property_type = PropertyType(prop_type_str)
        except ValueError:
            property_type = PropertyType.OTHER

    # Infer operation type from URL
    op_type = OperationType.SALE
    if "/arriendo/" in listing_url.lower():
        op_type = OperationType.RENT

    # ── Assemble listing ─────────────────────────────────────────────────────
    from src.normalization import generate_listing_fingerprint

    fingerprint = generate_listing_fingerprint(
        listing_id=listing_id,
        price=price_val,
        bedrooms=bedrooms,
        usable_area_m2=usable_area_m2,
        commune=commune,
    )

    return Listing(
        listing_id=listing_id,
        listing_url=listing_url,
        canonical_url=meta.get("og:url") or listing_url,
        search_url=search_url,
        title=title,
        publication_type=op_type,
        property_type=property_type,
        region=region,
        commune=commune,
        neighborhood=neighborhood,
        full_location_text=full_location_text,
        latitude=lat,
        longitude=lng,
        currency=currency,
        price=price_val,
        price_clp=price_clp,
        price_uf=price_uf,
        price_is_from=price_is_from,
        maintenance_fee=maint_fee,
        maintenance_fee_currency=maint_currency,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        parking_spaces=parking_spaces,
        storage_room=storage_room,
        usable_area_m2=usable_area_m2,
        total_area_m2=total_area_m2,
        land_area_m2=land_area_m2,
        year_built=year_built,
        orientation=orientation,
        furnished=furnished,
        condition=condition,
        seller_name=seller_name,
        seller_type=seller_type,
        agency_name=agency_name,
        description=description,
        features=features,
        image_urls=image_urls,
        listing_position=listing_position,
        scraped_at=now,
        published_at=published_at,
        first_seen_at=now,
        last_seen_at=now,
        is_active=True,
        raw_html_path=raw_html_path,
        raw_json=raw_json,
        fingerprint=fingerprint,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_attributes_table(rows: list[Node], tree: HTMLParser) -> dict[str, Any]:
    """Extract structured attributes from the spec table rows."""
    result: dict[str, Any] = {}
    features: list[str] = []

    # Also look for any table-like structure with th/td pairs
    all_tables = tree.css("table")
    for table in all_tables:
        for row in table.css("tr"):
            cells = row.css("th, td")
            if len(cells) >= 2:
                label = normalize_text(cells[0].text(strip=True)) or ""
                value = normalize_text(cells[1].text(strip=True)) or ""
                _assign_table_field(label.lower(), value, result, features)

    for row in rows:
        cells = row.css("th, td, .andes-table__header, .andes-table__column")
        if len(cells) >= 2:
            label = normalize_text(cells[0].text(strip=True)) or ""
            value = normalize_text(cells[1].text(strip=True)) or ""
            _assign_table_field(label.lower(), value, result, features)
        elif len(cells) == 1:
            text = normalize_text(cells[0].text(strip=True))
            if text:
                features.append(text)

    if features:
        result["features"] = features
    return result


def _assign_table_field(label: str, value: str, result: dict, features: list[str]) -> None:
    from src.normalization import parse_room_attributes, parse_area_m2, parse_bool

    if not value:
        return

    # Bedroom / bathroom / parking fields
    for keyword, (field, is_bool) in {
        "dormitorio": ("bedrooms", False),
        "habitacion": ("bedrooms", False),
        "baño": ("bathrooms", False),
        "bano": ("bathrooms", False),
        "estacionamiento": ("parking_spaces", False),
        "bodega": ("storage_room", True),
        "garage": ("parking_spaces", False),
    }.items():
        if keyword in label:
            if is_bool:
                result[field] = parse_bool(value) if value.strip().lower() not in ("1", "sí", "si", "yes") else True
            else:
                try:
                    result[field] = int(re.search(r"\d+", value).group())  # type: ignore[union-attr]
                except (AttributeError, ValueError):
                    pass
            return

    if any(w in label for w in ("superficie útil", "superficie util", "área útil", "m2 útil")):
        v = parse_area_m2(value)
        if v:
            result["usable_area_m2"] = v
    elif any(w in label for w in ("superficie total", "área total")):
        v = parse_area_m2(value)
        if v:
            result["total_area_m2"] = v
    elif any(w in label for w in ("terreno", "sitio")):
        v = parse_area_m2(value)
        if v:
            result["land_area_m2"] = v
    elif "año" in label and ("construc" in label or "edificac" in label):
        try:
            result["year_built"] = int(re.search(r"\d{4}", value).group())  # type: ignore[union-attr]
        except (AttributeError, ValueError):
            pass
    elif "orientac" in label:
        result["orientation"] = normalize_text(value)
    elif "amueblado" in label or "mobiliado" in label or "furnished" in label:
        result["furnished"] = parse_bool(value)
    elif "estado" in label or "condición" in label or "condicion" in label:
        from src.normalization import normalize_condition
        result["condition"] = normalize_condition(value)
    else:
        # Treat as a generic feature
        features.append(f"{label}: {value}")


def _extract_images(tree: HTMLParser) -> list[str]:
    """Extract all listing image URLs."""
    urls: list[str] = []
    seen: set[str] = set()

    for sel in load_selectors().get("detail", {}).get("images", []):
        try:
            for img in tree.css(sel):
                for attr in ("src", "data-src", "data-zoom", "data-original"):
                    src = img.attributes.get(attr) or ""
                    if src and src not in seen and not src.endswith(".gif"):
                        if src.startswith("//"):
                            src = "https:" + src
                        seen.add(src)
                        urls.append(src)
        except Exception:
            continue

    return urls


def _ld_location(ld: dict[str, Any]) -> Optional[str]:
    loc = ld.get("address", {})
    if isinstance(loc, dict):
        parts = [
            loc.get("streetAddress"),
            loc.get("addressLocality"),
            loc.get("addressRegion"),
        ]
        parts = [p for p in parts if p]
        return ", ".join(parts) if parts else None
    return str(loc) if loc else None


def _classify_seller(
    badge_text: Optional[str],
    seller_name: Optional[str],
    agency_name: Optional[str],
) -> SellerType:
    combined = " ".join(filter(None, [badge_text, seller_name, agency_name])).lower()
    if any(w in combined for w in ("inmobiliaria", "agencia", "agency", "constructora", "empresa")):
        return SellerType.AGENCY
    if any(w in combined for w in ("proyecto", "developer", "desarrolladora")):
        return SellerType.DEVELOPER
    if any(w in combined for w in ("dueño", "dueno", "propietario", "particular", "owner")):
        return SellerType.OWNER
    return SellerType.UNKNOWN
