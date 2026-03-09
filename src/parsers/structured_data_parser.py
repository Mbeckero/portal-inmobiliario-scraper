"""Extract structured data from page source before touching CSS selectors.

Priority order:
  1. window.__INITIAL_STATE__ / APP_STATE (MercadoLibre JS state)
  2. JSON-LD <script type="application/ld+json">
  3. OpenGraph / meta tags
"""
from __future__ import annotations

import json
import re
from typing import Any, Optional

from src.logging_config import get_logger

logger = get_logger(__name__)

# Keys that MercadoLibre may use for page state
_WINDOW_STATE_KEYS = (
    "__INITIAL_STATE__",
    "initialState",
    "serverState",
    "APP_STATE",
    "MeliGA",
)


def extract_window_state(html: str) -> Optional[dict[str, Any]]:
    """Try to extract the largest window-level JSON state object from the page source.

    Returns the parsed dict, or None if not found.
    """
    for key in _WINDOW_STATE_KEYS:
        # Pattern: window.__KEY__ = {...}; or window["__KEY__"] = {...};
        for pattern in (
            rf'window\.{re.escape(key)}\s*=\s*(\{{.*?\}});',
            rf'window\["{re.escape(key)}"\]\s*=\s*(\{{.*?\}});',
        ):
            m = re.search(pattern, html, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                    logger.debug("window_state_found", key=key, keys=list(data.keys())[:10])
                    return data
                except json.JSONDecodeError:
                    continue

    # Broader fallback: find any large JSON blob assigned to a variable
    m = re.search(r'var\s+\w+\s*=\s*(\{["\']id["\']:\s*["\']MLC)', html)
    if m:
        # Try to extract the full JSON from there
        start = m.start(1)
        candidate = _extract_balanced_json(html, start)
        if candidate:
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass

    return None


def extract_json_ld(html: str) -> list[dict[str, Any]]:
    """Extract all JSON-LD blocks from the page."""
    results: list[dict[str, Any]] = []
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.DOTALL | re.IGNORECASE,
    ):
        try:
            data = json.loads(m.group(1))
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)
        except json.JSONDecodeError:
            continue
    return results


def extract_meta_tags(html: str) -> dict[str, str]:
    """Extract OpenGraph and standard meta tags."""
    tags: dict[str, str] = {}
    for m in re.finditer(
        r'<meta\s+(?:property|name)=["\']([^"\']+)["\']\s+content=["\']([^"\']*)["\']',
        html,
        re.IGNORECASE,
    ):
        tags[m.group(1)] = m.group(2)
    # Also handle content-first ordering
    for m in re.finditer(
        r'<meta\s+content=["\']([^"\']*)["\']\s+(?:property|name)=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    ):
        tags[m.group(2)] = m.group(1)
    return tags


def extract_listing_id_from_state(state: dict[str, Any]) -> Optional[str]:
    """Try to find the listing ID inside the window state object."""
    # Common MercadoLibre paths
    for path in (
        ["initialState", "id"],
        ["id"],
        ["item", "id"],
        ["initialState", "item", "id"],
    ):
        node: Any = state
        for key in path:
            if isinstance(node, dict) and key in node:
                node = node[key]
            else:
                node = None
                break
        if node and isinstance(node, str) and node.upper().startswith("MLC"):
            return node.upper()

    # Deep search for MLC ID
    return _deep_find_mlc_id(state)


def _deep_find_mlc_id(obj: Any, depth: int = 0) -> Optional[str]:
    if depth > 5:
        return None
    if isinstance(obj, str) and re.match(r"MLC-?\d+", obj, re.IGNORECASE):
        return obj.upper()
    if isinstance(obj, dict):
        for v in list(obj.values())[:20]:
            result = _deep_find_mlc_id(v, depth + 1)
            if result:
                return result
    if isinstance(obj, list):
        for item in obj[:5]:
            result = _deep_find_mlc_id(item, depth + 1)
            if result:
                return result
    return None


def extract_coordinates_from_state(state: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """Try to extract lat/lng from the window state."""
    # Search for location objects
    lat = _deep_find_key(state, "latitude") or _deep_find_key(state, "lat")
    lng = _deep_find_key(state, "longitude") or _deep_find_key(state, "lng") or _deep_find_key(state, "lon")

    try:
        return float(lat) if lat is not None else None, float(lng) if lng is not None else None
    except (TypeError, ValueError):
        return None, None


def _deep_find_key(obj: Any, key: str, depth: int = 0) -> Any:
    if depth > 6:
        return None
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in list(obj.values())[:30]:
            result = _deep_find_key(v, key, depth + 1)
            if result is not None:
                return result
    if isinstance(obj, list):
        for item in obj[:10]:
            result = _deep_find_key(item, key, depth + 1)
            if result is not None:
                return result
    return None


def _extract_balanced_json(html: str, start: int, max_length: int = 200_000) -> Optional[str]:
    """Extract a balanced {...} JSON string starting at `start`."""
    depth = 0
    in_string = False
    escape_next = False
    end = min(start + max_length, len(html))

    for i in range(start, end):
        ch = html[i]
        if escape_next:
            escape_next = False
            continue
        if ch == "\\" and in_string:
            escape_next = True
            continue
        if ch == '"' and not escape_next:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return html[start : i + 1]

    return None
