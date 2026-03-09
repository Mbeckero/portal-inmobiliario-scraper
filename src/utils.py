"""General-purpose utilities shared across modules."""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import random
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

from src.logging_config import get_logger

logger = get_logger(__name__)


# ── Text helpers ──────────────────────────────────────────────────────────────

def normalize_text(text: str | None) -> str | None:
    """Strip extra whitespace and normalize unicode."""
    if text is None:
        return None
    text = unicodedata.normalize("NFC", text)
    return re.sub(r"\s+", " ", text).strip() or None


def slugify(text: str) -> str:
    """ASCII slug for use in file names."""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", text)


def truncate(text: str | None, max_len: int = 500) -> str | None:
    if text and len(text) > max_len:
        return text[:max_len] + "…"
    return text


# ── Hashing / fingerprinting ──────────────────────────────────────────────────

def sha256_hex(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def stable_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)


# ── ID extraction from Portal Inmobiliario URLs ───────────────────────────────

_MLC_RE = re.compile(r"/?(MLC-?\d+)", re.IGNORECASE)


def extract_listing_id(url: str) -> str | None:
    """Extract 'MLC-XXXXXXXXXX' from a Portal Inmobiliario URL."""
    m = _MLC_RE.search(url)
    if m:
        return m.group(1).upper().replace("MLC", "MLC-").replace("MLC--", "MLC-")
    # Fallback: hash of the URL
    return None


def url_to_id(url: str) -> str:
    """Return a stable ID for any URL, using MLC ID when present."""
    mlc = extract_listing_id(url)
    if mlc:
        return mlc
    return "URL-" + sha256_hex(url)[:16]


# ── Async delay helpers ───────────────────────────────────────────────────────

async def random_delay(min_s: float = 2.0, max_s: float = 5.0) -> None:
    delay = random.uniform(min_s, max_s)
    logger.debug("rate_limit_delay", seconds=round(delay, 2))
    await asyncio.sleep(delay)


# ── File I/O helpers ──────────────────────────────────────────────────────────

def save_raw_html(html: str, url: str, run_id: str, base_dir: Path) -> Path:
    """Save raw HTML gzip-compressed; return the file path."""
    date_str = datetime.utcnow().strftime("%Y%m%d")
    listing_id = slugify(url_to_id(url) or sha256_hex(url)[:12])
    out_dir = base_dir / date_str / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{listing_id}.html.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write(html)
    return path


def save_raw_json(data: dict, url: str, run_id: str, base_dir: Path) -> Path:
    """Save raw JSON payload; return the file path."""
    date_str = datetime.utcnow().strftime("%Y%m%d")
    listing_id = slugify(url_to_id(url) or sha256_hex(url)[:12])
    out_dir = base_dir / date_str / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{listing_id}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_urls_from_file(path: Path) -> list[str]:
    """Load one URL per line from a text file, ignoring blank lines and comments."""
    urls: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def load_checkpoint(path: Path) -> set[str]:
    """Load a set of already-processed URLs from a checkpoint file."""
    if not path.exists():
        return set()
    return set(path.read_text(encoding="utf-8").splitlines())


def save_checkpoint(path: Path, urls: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(sorted(urls)), encoding="utf-8")


def append_checkpoint(path: Path, url: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(url + "\n")


# ── Date parsing ──────────────────────────────────────────────────────────────

_SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def parse_spanish_date(text: str) -> datetime | None:
    """Try to parse Spanish date strings like '15 de marzo de 2024'."""
    if not text:
        return None
    text = text.lower().strip()
    # ISO datetime
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    # "15 de marzo de 2024"
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", text)
    if m:
        day, month_str, year = int(m.group(1)), m.group(2), int(m.group(3))
        month = _SPANISH_MONTHS.get(month_str)
        if month:
            try:
                return datetime(year, month, day)
            except ValueError:
                pass
    # Relative: "hace X días / meses"
    m = re.search(r"hace\s+(\d+)\s+(día|dias|día|mes|meses|semana|semanas|hora|horas)", text)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        from datetime import timedelta
        now = datetime.utcnow()
        if "día" in unit or "dia" in unit:
            return now - timedelta(days=n)
        if "semana" in unit:
            return now - timedelta(weeks=n)
        if "mes" in unit:
            return now - timedelta(days=n * 30)
        if "hora" in unit:
            return now - timedelta(hours=n)
    return None
