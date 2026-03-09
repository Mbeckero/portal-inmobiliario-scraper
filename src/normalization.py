"""Chile-specific normalization functions.

All functions are pure / side-effect-free so they are easy to unit-test.
"""
from __future__ import annotations

import re
from typing import Optional

from src.logging_config import get_logger
from src.utils import normalize_text

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Currency symbols → canonical codes
CURRENCY_MAP: dict[str, str] = {
    "$": "CLP",
    "clp": "CLP",
    "uf": "UF",
    "clf": "UF",
    "us$": "USD",
    "usd": "USD",
}

# Spanish area unit aliases → multiplier to m²
AREA_UNIT_MAP: dict[str, float] = {
    "m2": 1.0,
    "m²": 1.0,
    "m": 1.0,         # bare "m" sometimes used
    "mt2": 1.0,
    "mt²": 1.0,
    "mts2": 1.0,
    "mts²": 1.0,
    "ha": 10_000.0,
    "hectárea": 10_000.0,
    "hectarea": 10_000.0,
}

# Chilean region normalization (partial text match)
REGION_NORMALIZATION: list[tuple[str, str]] = [
    ("metropolitana", "Región Metropolitana"),
    ("valparaíso", "Valparaíso"),
    ("valparaiso", "Valparaíso"),
    ("biobío", "Biobío"),
    ("biobio", "Biobío"),
    ("araucanía", "Araucanía"),
    ("araucania", "Araucanía"),
    ("los lagos", "Los Lagos"),
    ("coquimbo", "Coquimbo"),
    ("antofagasta", "Antofagasta"),
    ("maule", "Maule"),
    ("o'higgins", "O'Higgins"),
    ("ohiggins", "O'Higgins"),
    ("aysén", "Aysén"),
    ("aysen", "Aysén"),
    ("magallanes", "Magallanes"),
    ("atacama", "Atacama"),
    ("tarapacá", "Tarapacá"),
    ("tarapaca", "Tarapacá"),
    ("arica", "Arica y Parinacota"),
    ("los ríos", "Los Ríos"),
    ("los rios", "Los Ríos"),
    ("ñuble", "Ñuble"),
    ("nuble", "Ñuble"),
]


# ── Price normalization ───────────────────────────────────────────────────────

def parse_price(text: str | None) -> tuple[float | None, str | None, bool]:
    """Parse a price string into (numeric_value, currency_code, is_from).

    Returns:
        (value, currency, is_from)
        - value: float or None
        - currency: "CLP" | "UF" | "USD" | None
        - is_from: True when the listing says "Desde" (starting from)

    Examples:
        "$ 45.000.000"          → (45_000_000.0, "CLP", False)
        "UF 4.500"              → (4_500.0, "UF", False)
        "Desde UF 3.200"        → (3_200.0, "UF", True)
        "$ 2.500 UF"            → (2_500.0, "UF", False)  ← unusual format
        "45000000"              → (45_000_000.0, None, False)
    """
    if not text:
        return None, None, False

    text = normalize_text(text) or ""
    is_from = bool(re.search(r"\bdesde\b", text, re.IGNORECASE))
    text_clean = re.sub(r"\bdesde\b", "", text, flags=re.IGNORECASE).strip()

    # Detect currency
    currency: str | None = None
    for symbol, code in CURRENCY_MAP.items():
        pattern = re.escape(symbol)
        if re.search(pattern, text_clean, re.IGNORECASE):
            currency = code
            text_clean = re.sub(pattern, "", text_clean, flags=re.IGNORECASE).strip()
            break

    # Strip non-numeric (except decimal separators)
    # Chilean number format: dots as thousands, comma as decimal
    # e.g. "45.000.000" = 45,000,000  |  "4.500,50" = 4500.50
    # Extract the first number-like sequence
    m = re.search(r"[\d.,]+", text_clean.replace(" ", ""))
    if not m:
        return None, currency, is_from

    num_str = m.group(0)

    try:
        value = _parse_chilean_number(num_str)
    except ValueError:
        logger.warning("price_parse_failed", raw=text)
        return None, currency, is_from

    return value, currency, is_from


def _parse_chilean_number(s: str) -> float:
    """Parse Chilean-formatted number string to float.

    Chilean convention: dots = thousands separator, comma = decimal.
    BUT: a single dot followed by 1-2 digits is treated as a decimal separator
    (e.g. "42.58" → 42.58 m², "6.149" → 6149 UF, "1.200" → 1200 m²).
    """
    s = s.strip()
    # Comma as decimal separator (e.g. "4.500,50" or "0,5")
    m = re.match(r"^([\d.]*),(\d+)$", s)
    if m:
        integer_part = m.group(1).replace(".", "")
        decimal_part = m.group(2)
        return float(f"{integer_part or '0'}.{decimal_part}")
    # Single dot: check whether decimal or thousands
    dot_count = s.count(".")
    if dot_count == 1:
        after_dot = s.split(".")[1]
        if len(after_dot) <= 2:
            # Decimal separator: "42.58" → 42.58, "6.5" → 6.5
            return float(s)
        # Thousands separator: "6.149" → 6149, "1.200" → 1200
        return float(s.replace(".", ""))
    # Multiple dots: all are thousands separators ("45.000.000" → 45000000)
    return float(s.replace(".", ""))


def parse_expenses(text: str | None) -> tuple[float | None, str | None]:
    """Parse maintenance fee / gastos comunes.

    Returns (amount, currency).
    """
    if not text:
        return None, None
    text = normalize_text(text) or ""
    # Remove labels
    text = re.sub(
        r"gastos?\s+comunes?|expensas?|mantenimiento|administración", "", text, flags=re.IGNORECASE
    ).strip()
    value, currency, _ = parse_price(text)
    return value, currency


# ── Area normalization ────────────────────────────────────────────────────────

def parse_area_m2(text: str | None) -> float | None:
    """Parse an area string to m².

    Examples:
        "85 m²"         → 85.0
        "1.200 mt2"     → 1200.0
        "0,5 ha"        → 5000.0
        "Total: 120m2"  → 120.0
    """
    if not text:
        return None
    text = normalize_text(text) or ""

    # Match: number (with optional dot/comma separators) followed by optional space + unit
    m = re.search(r"([\d.,]+)\s*(m²|m2|mt2|mt²|mts2|mts²|ha|hectárea|hectarea|m\b)", text, re.IGNORECASE)
    if not m:
        # Bare number?
        m2 = re.search(r"([\d.,]+)", text)
        if m2:
            try:
                return _parse_chilean_number(m2.group(1))
            except ValueError:
                return None
        return None

    num_str, unit = m.group(1), m.group(2).lower()
    try:
        value = _parse_chilean_number(num_str)
    except ValueError:
        return None

    multiplier = AREA_UNIT_MAP.get(unit, 1.0)
    return round(value * multiplier, 2)


# ── Bedroom / bathroom normalization ─────────────────────────────────────────

# Spanish attribute keywords → (field_name, is_boolean)
ATTRIBUTE_MAP: dict[str, tuple[str, bool]] = {
    "dormitorio": ("bedrooms", False),
    "habitacion": ("bedrooms", False),
    "recamara": ("bedrooms", False),
    "pieza": ("bedrooms", False),
    "baño": ("bathrooms", False),
    "bano": ("bathrooms", False),
    "estacionamiento": ("parking_spaces", False),
    "garage": ("parking_spaces", False),
    "bodega": ("storage_room", True),
    "suite": ("bedrooms", False),
}


def parse_bedrooms_bathrooms(attribute_text: str) -> dict[str, int | bool]:
    """Parse a short attribute chip like '3 Dormitorios', '2 Baños', '1 Bodega'.

    Returns a dict with at most one key/value pair.
    """
    result: dict[str, int | bool] = {}
    if not attribute_text:
        return result

    text = normalize_text(attribute_text) or ""
    text_lower = text.lower()

    for keyword, (field, is_bool) in ATTRIBUTE_MAP.items():
        if keyword in text_lower:
            if is_bool:
                result[field] = True
            else:
                # Extract leading number
                m = re.search(r"(\d+)", text)
                if m:
                    result[field] = int(m.group(1))
            break

    return result


def parse_room_attributes(raw_attributes: list[str]) -> dict[str, int | bool | None]:
    """Aggregate multiple attribute chips into a rooms/parking dict."""
    merged: dict[str, int | bool | None] = {}
    for attr in raw_attributes:
        parsed = parse_bedrooms_bathrooms(attr)
        merged.update(parsed)
    return merged


# ── Location normalization ────────────────────────────────────────────────────

def parse_location_components(
    location_text: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Split a Chilean location string into (region, commune, neighborhood).

    Portal Inmobiliario typically shows: "Commune, Region"
    e.g. "Las Condes, Región Metropolitana"
         "Reñaca, Viña del Mar, Valparaíso"

    Returns: (region, commune, neighborhood)
    """
    if not location_text:
        return None, None, None

    parts = [p.strip() for p in location_text.split(",")]
    parts = [p for p in parts if p]

    region: str | None = None
    commune: str | None = None
    neighborhood: str | None = None

    for part in reversed(parts):
        part_lower = part.lower()
        # Region detection
        for keyword, normalized in REGION_NORMALIZATION:
            if keyword in part_lower:
                region = normalized
                break

    # Assign commune and neighborhood from remaining parts
    non_region = [p for p in parts if not _is_region_text(p.lower())]
    if len(non_region) >= 1:
        commune = non_region[-1]  # last non-region is typically the commune
    if len(non_region) >= 2:
        neighborhood = non_region[0]

    return region, commune, neighborhood


def _is_region_text(text: str) -> bool:
    return any(kw in text for kw, _ in REGION_NORMALIZATION)


def normalize_region(region_text: str | None) -> str | None:
    if not region_text:
        return None
    text_lower = region_text.lower()
    for keyword, normalized in REGION_NORMALIZATION:
        if keyword in text_lower:
            return normalized
    return normalize_text(region_text)


# ── Boolean normalization ─────────────────────────────────────────────────────

_YES_WORDS = {"sí", "si", "yes", "true", "1", "verdadero", "con"}
_NO_WORDS = {"no", "false", "0", "falso", "sin"}


def parse_bool(text: str | None) -> bool | None:
    if text is None:
        return None
    t = text.strip().lower()
    if t in _YES_WORDS:
        return True
    if t in _NO_WORDS:
        return False
    return None


# ── Condition normalization ───────────────────────────────────────────────────

def normalize_condition(text: str | None) -> str | None:
    """Normalize property condition: 'new' | 'used' | 'project' | None."""
    if not text:
        return None
    t = text.lower()
    if any(w in t for w in ("nuevo", "nueva", "estreno", "new")):
        return "new"
    if any(w in t for w in ("usado", "usada", "segunda mano", "used")):
        return "used"
    if any(w in t for w in ("proyecto", "en construcción", "en construccion", "preventa")):
        return "project"
    return normalize_text(text)


# ── Property type normalization ───────────────────────────────────────────────

def normalize_property_type(text: str | None) -> Optional[str]:
    """Map raw property type text to a canonical value."""
    if not text:
        return None
    t = text.lower()
    if any(w in t for w in ("departamento", "depto", "apt", "apartamento")):
        return "apartment"
    if any(w in t for w in ("casa", "house", "villa")):
        return "house"
    if any(w in t for w in ("terreno", "parcela", "sitio", "land")):
        return "land"
    if any(w in t for w in ("oficina", "office")):
        return "office"
    if any(w in t for w in ("local comercial", "comercial", "local")):
        return "commercial"
    if any(w in t for w in ("bodega", "warehouse", "storage")):
        return "warehouse"
    if any(w in t for w in ("estacionamiento", "parking", "garaje")):
        return "parking"
    return "other"


# ── Fingerprint ───────────────────────────────────────────────────────────────

def generate_listing_fingerprint(
    listing_id: str,
    price: float | None,
    bedrooms: int | None,
    usable_area_m2: float | None,
    commune: str | None,
) -> str:
    """Stable SHA-256 fingerprint for deduplication.

    Uses only stable identity fields — not scraped_at or other volatile fields.
    """
    from src.utils import sha256_hex, stable_json

    payload = stable_json(
        {
            "listing_id": listing_id,
            "price": price,
            "bedrooms": bedrooms,
            "usable_area_m2": usable_area_m2,
            "commune": commune,
        }
    )
    return sha256_hex(payload)[:32]
