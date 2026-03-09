"""Unit tests for the search-results page parser."""
from __future__ import annotations

import pytest

from src.parsers.search_parser import parse_search_page, parse_total_results


class TestParseSearchPage:
    def test_returns_three_cards(self, search_page_html):
        cards, next_url = parse_search_page(search_page_html, search_url="https://example.com")
        assert len(cards) == 3

    def test_first_card_id(self, search_page_html):
        cards, _ = parse_search_page(search_page_html)
        assert "MLC" in cards[0].listing_id.upper()

    def test_first_card_url(self, search_page_html):
        cards, _ = parse_search_page(search_page_html)
        assert "portalinmobiliario.com" in cards[0].listing_url
        assert "MLC-123456789" in cards[0].listing_url

    def test_first_card_title(self, search_page_html):
        cards, _ = parse_search_page(search_page_html)
        assert cards[0].title is not None
        assert "Departamento" in cards[0].title or "departamento" in cards[0].title.lower()

    def test_uf_price(self, search_page_html):
        cards, _ = parse_search_page(search_page_html)
        assert cards[0].price == 8500.0
        assert cards[0].currency == "UF"

    def test_second_card_price(self, search_page_html):
        cards, _ = parse_search_page(search_page_html)
        assert cards[1].price == 25_000.0
        assert cards[1].currency == "UF"

    def test_clp_price(self, search_page_html):
        cards, _ = parse_search_page(search_page_html)
        # Third card is CLP
        assert cards[2].price == 85_000_000.0
        assert cards[2].currency == "CLP"

    def test_location_text(self, search_page_html):
        cards, _ = parse_search_page(search_page_html)
        assert cards[0].location_text is not None
        assert "Las Condes" in cards[0].location_text

    def test_attributes_raw(self, search_page_html):
        cards, _ = parse_search_page(search_page_html)
        attrs = cards[0].attributes_raw
        assert len(attrs) >= 2
        # Should contain bedroom/bathroom info
        combined = " ".join(attrs)
        assert "Dormitorio" in combined or "dormitorio" in combined.lower()

    def test_next_page_url(self, search_page_html):
        _, next_url = parse_search_page(search_page_html)
        assert next_url is not None
        assert "_Desde_49" in next_url

    def test_listing_position(self, search_page_html):
        cards, _ = parse_search_page(search_page_html)
        assert cards[0].listing_position == 1
        assert cards[1].listing_position == 2
        assert cards[2].listing_position == 3

    def test_search_url_stored(self, search_page_html):
        url = "https://pi.com/venta/departamento/las-condes"
        cards, _ = parse_search_page(search_page_html, search_url=url)
        assert cards[0].search_url == url

    def test_empty_html(self):
        cards, next_url = parse_search_page("<html><body></body></html>")
        assert cards == []
        assert next_url is None


class TestParseTotalResults:
    def test_extracts_count(self, search_page_html):
        total = parse_total_results(search_page_html)
        assert total == 1243

    def test_empty_html(self):
        total = parse_total_results("<html><body></body></html>")
        assert total is None
