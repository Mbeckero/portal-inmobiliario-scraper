"""Shared pytest fixtures."""
from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def search_page_html() -> str:
    return (FIXTURES_DIR / "search_page.html").read_text(encoding="utf-8")


@pytest.fixture
def detail_page_html() -> str:
    return (FIXTURES_DIR / "detail_page.html").read_text(encoding="utf-8")


@pytest.fixture
def sample_listing_url() -> str:
    return "https://www.portalinmobiliario.com/MLC-123456789-departamento-3-dormitorios-las-condes_JM"


@pytest.fixture
def sample_search_url() -> str:
    return "https://www.portalinmobiliario.com/venta/departamento/las-condes-metropolitana"
