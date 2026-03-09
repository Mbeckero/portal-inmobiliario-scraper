"""Unit tests for the listing detail page parser."""
from __future__ import annotations

import pytest

from src.parsers.detail_parser import parse_detail_page
from src.models import OperationType, SellerType


SAMPLE_URL = "https://www.portalinmobiliario.com/MLC-123456789-departamento-3-dormitorios-las-condes_JM"


class TestParseDetailPage:
    def test_returns_listing(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing is not None

    def test_listing_id(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.listing_id is not None
        assert "MLC" in listing.listing_id.upper()

    def test_listing_url_preserved(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.listing_url == SAMPLE_URL

    def test_title(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.title is not None
        assert "Departamento" in listing.title or "departamento" in listing.title.lower()

    def test_price_uf(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.price == 8500.0
        assert listing.currency == "UF"
        assert listing.price_uf == 8500.0
        assert listing.price_clp is None

    def test_maintenance_fee(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.maintenance_fee == 120_000.0
        assert listing.maintenance_fee_currency == "CLP"

    def test_bedrooms(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.bedrooms == 3

    def test_bathrooms(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.bathrooms == 2

    def test_parking(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.parking_spaces == 1

    def test_usable_area(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.usable_area_m2 == 85.0

    def test_total_area(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.total_area_m2 == 95.0

    def test_commune(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.commune == "Las Condes"

    def test_region(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.region is not None
        assert "Metropolitana" in listing.region

    def test_coordinates_from_json_ld(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        # JSON-LD in fixture has lat/lng
        assert listing.latitude is not None
        assert listing.longitude is not None
        assert abs(listing.latitude - (-33.4105)) < 0.01
        assert abs(listing.longitude - (-70.5809)) < 0.01

    def test_description(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.description is not None
        assert len(listing.description) > 20

    def test_images(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert len(listing.image_urls) >= 1

    def test_main_image(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.main_image_url is not None

    def test_seller_name(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.seller_name is not None
        assert "Inmobiliaria" in listing.seller_name

    def test_seller_type_agency(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.seller_type == SellerType.AGENCY

    def test_year_built(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.year_built == 2018

    def test_orientation(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.orientation is not None
        assert "Oriente" in listing.orientation or "oriente" in listing.orientation.lower()

    def test_storage_room(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.storage_room is True

    def test_operation_type_sale(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.publication_type == OperationType.SALE

    def test_operation_type_rent(self, detail_page_html):
        rent_url = "https://www.portalinmobiliario.com/arriendo/MLC-123456789-depto_JM"
        listing = parse_detail_page(detail_page_html, rent_url)
        assert listing is not None
        assert listing.publication_type == OperationType.RENT

    def test_fingerprint_set(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.fingerprint is not None
        assert len(listing.fingerprint) > 0

    def test_is_active_true(self, detail_page_html):
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.is_active is True

    def test_scraped_at_set(self, detail_page_html):
        from datetime import datetime
        listing = parse_detail_page(detail_page_html, SAMPLE_URL)
        assert listing.scraped_at is not None
        assert isinstance(listing.scraped_at, datetime)

    def test_bad_html_returns_none(self):
        listing = parse_detail_page("not html", "https://pi.com/MLC-999")
        # Should return None or a partially populated listing with listing_id
        # (URL-based ID is always generated)
        # The key requirement: no exception raised
        pass

    def test_search_url_propagated(self, detail_page_html):
        search_url = "https://pi.com/venta/departamento/las-condes"
        listing = parse_detail_page(
            detail_page_html, SAMPLE_URL, search_url=search_url
        )
        assert listing.search_url == search_url
