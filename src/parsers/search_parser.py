"""Parse Portal Inmobiliario search-result (listing-index) pages.

Extraction strategy:
  1. Use selectolax (HTMLParser) for fast CSS-selector extraction.
  2. All selectors come from configs/selectors.yaml — never hardcoded here.
  3. Implement fallback chains: try each selector in order, use first match.
"""
from __future__ import annotations

import re
from typing import Optional

from selectolax.parser import HTMLParser, Node

from src.config import load_selectors
from src.logging_config import get_logger
from src.models import SearchCard
from src.normalization import parse_price
from src.utils import normalize_text, url_to_id

logger = get_logger(__name__)


def _sel(group: str, key: str) -> list[str]:
    """Retrieve selector list from YAML config."""
    selectors = load_selectors()
    value = selectors.get(group, {}).get(key, [])
    if isinstance(value, str):
        return [value]
    return list(value)


def _first_match(node: Node | HTMLParser, selectors: list[str]) -> Optional[Node]:
    """Return the first node matching any selector in the list."""
    for sel in selectors:
        try:
            result = node.css_first(sel)
            if result is not None:
                return result
        except Exception:
            continue
    return None


def _all_matches(node: Node | HTMLParser, selectors: list[str]) -> list[Node]:
    """Return all nodes matching the first selector that yields results."""
    for sel in selectors:
        try:
            results = node.css(sel)
            if results:
                return results
        except Exception:
            continue
    return []


def _text(node: Optional[Node]) -> Optional[str]:
    if node is None:
        return None
    return normalize_text(node.text(strip=True))


def _attr(node: Node, name: str) -> str:
    """Safe attribute read: returns '' when attribute is absent OR valueless."""
    return node.attributes.get(name) or ""


def parse_search_page(html: str, search_url: str = "") -> tuple[list[SearchCard], Optional[str]]:
    """Parse a search-results HTML page.

    Returns:
        (cards, next_page_url)
        - cards: list of SearchCard (lightweight pre-enrichment objects)
        - next_page_url: URL of next page, or None if last page
    """
    tree = HTMLParser(html)
    cards: list[SearchCard] = []

    # ── Find listing items ────────────────────────────────────────────────────
    items = _all_matches(tree, _sel("search", "listing_item"))
    if not items:
        logger.warning("search_no_items_found", url=search_url, html_snippet=html[:500])
        return [], None

    logger.info("search_items_found", count=len(items), url=search_url)

    for position, item in enumerate(items, start=1):
        try:
            card = _parse_card(item, position=position, search_url=search_url)
            if card:
                cards.append(card)
        except Exception as exc:
            logger.warning("card_parse_error", position=position, error=str(exc))

    # ── Pagination ────────────────────────────────────────────────────────────
    next_page_url = _extract_next_page(tree)

    return cards, next_page_url


def _parse_card(item: Node, position: int, search_url: str) -> Optional[SearchCard]:
    """Extract a SearchCard from a single listing-card node."""
    # Listing URL
    link_node = _first_match(item, _sel("search", "listing_link"))
    if link_node is None:
        # Try any <a> with an MLC href
        for a in item.css("a[href]"):
            href = _attr(a, "href")
            if "MLC" in href.upper() or "/venta/" in href or "/arriendo/" in href:
                link_node = a
                break
    if link_node is None:
        logger.debug("card_no_link", position=position)
        return None

    listing_url = _attr(link_node, "href").strip()
    if not listing_url:
        return None
    # Ensure absolute URL
    if listing_url.startswith("//"):
        listing_url = "https:" + listing_url
    elif listing_url.startswith("/"):
        listing_url = "https://www.portalinmobiliario.com" + listing_url

    listing_id = url_to_id(listing_url)

    # Title
    title_node = _first_match(item, _sel("search", "listing_title"))
    title = _text(title_node)

    # Price
    price_container = _first_match(item, _sel("search", "price_container"))
    price_val: Optional[float] = None
    currency: Optional[str] = None
    if price_container:
        raw_price_text = price_container.text(strip=True)
        price_val, currency, _ = parse_price(raw_price_text)
    if price_val is None:
        # Try fraction + symbol separately
        fraction = _first_match(item, _sel("search", "price_fraction"))
        symbol = _first_match(item, _sel("search", "price_symbol"))
        raw = ""
        if symbol:
            raw += _text(symbol) or ""
        if fraction:
            raw += " " + (_text(fraction) or "")
        if raw.strip():
            price_val, currency, _ = parse_price(raw)

    # Location
    loc_node = _first_match(item, _sel("search", "location_text"))
    location_text = _text(loc_node)

    # Attribute chips (bedrooms, area, etc.)
    attr_nodes = _all_matches(item, _sel("search", "attribute_items"))
    attributes_raw = [normalize_text(n.text(strip=True)) for n in attr_nodes if n.text(strip=True)]
    attributes_raw = [a for a in attributes_raw if a]

    # Thumbnail
    thumbnail_url: Optional[str] = None
    img = item.css_first("img[src]")
    if img:
        src = _attr(img, "src") or _attr(img, "data-src")
        if src and not src.endswith(".gif"):
            thumbnail_url = src

    return SearchCard(
        listing_id=listing_id,
        listing_url=listing_url,
        title=title,
        price=price_val,
        currency=currency,
        location_text=location_text,
        attributes_raw=attributes_raw,
        listing_position=position,
        search_url=search_url,
        thumbnail_url=thumbnail_url,
    )


def _extract_next_page(tree: HTMLParser) -> Optional[str]:
    """Return the URL for the next page of results, or None."""
    for sel in _sel("search", "next_page"):
        node = tree.css_first(sel)
        if node:
            href = _attr(node, "href").strip()
            if href and href != "#":
                if href.startswith("//"):
                    return "https:" + href
                if href.startswith("/"):
                    return "https://www.portalinmobiliario.com" + href
                return href
    return None


def parse_total_results(html: str) -> Optional[int]:
    """Extract total result count from the search page."""
    tree = HTMLParser(html)
    for sel in _sel("search", "total_results"):
        node = tree.css_first(sel)
        if node:
            text = node.text(strip=True)
            m = re.search(r"[\d.,]+", text.replace("\xa0", ""))
            if m:
                try:
                    return int(m.group(0).replace(".", "").replace(",", ""))
                except ValueError:
                    continue
    return None
