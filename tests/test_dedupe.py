"""Unit tests for deduplication and change detection."""
from __future__ import annotations

from datetime import datetime

import pytest

from src.dedupe import compute_fingerprint, detect_changes, is_duplicate, merge_listing
from src.models import Listing


def _make_listing(**kwargs) -> Listing:
    defaults = dict(
        listing_id="MLC-123456789",
        listing_url="https://www.portalinmobiliario.com/MLC-123456789",
        title="Departamento Las Condes",
        price=8500.0,
        currency="UF",
        price_uf=8500.0,
        bedrooms=3,
        bathrooms=2,
        usable_area_m2=85.0,
        commune="Las Condes",
        is_active=True,
        scraped_at=datetime(2024, 1, 15, 10, 0, 0),
    )
    defaults.update(kwargs)
    return Listing(**defaults)


class TestComputeFingerprint:
    def test_sets_fingerprint(self):
        listing = _make_listing()
        fp = compute_fingerprint(listing)
        assert listing.fingerprint is not None
        assert len(listing.fingerprint) > 0
        assert listing.fingerprint == fp

    def test_deterministic(self):
        a = _make_listing()
        b = _make_listing()
        assert compute_fingerprint(a) == compute_fingerprint(b)

    def test_changes_on_price(self):
        a = _make_listing(price=8500.0, price_uf=8500.0)
        b = _make_listing(price=9000.0, price_uf=9000.0)
        assert compute_fingerprint(a) != compute_fingerprint(b)


class TestIsDuplicate:
    def test_same_listing_id(self):
        a = _make_listing()
        b = _make_listing()
        compute_fingerprint(a)
        compute_fingerprint(b)
        assert is_duplicate(a, b) is True

    def test_different_listing_id(self):
        a = _make_listing(listing_id="MLC-111", listing_url="https://pi.com/MLC-111")
        b = _make_listing(listing_id="MLC-222", listing_url="https://pi.com/MLC-222")
        compute_fingerprint(a)
        compute_fingerprint(b)
        assert is_duplicate(a, b) is False


class TestDetectChanges:
    def test_no_changes(self):
        old = _make_listing()
        new = _make_listing()
        compute_fingerprint(old)
        compute_fingerprint(new)
        changes = detect_changes(new, old)
        assert changes == []

    def test_price_change(self):
        old = _make_listing(price=8500.0, price_uf=8500.0)
        new = _make_listing(price=8000.0, price_uf=8000.0)
        changes = detect_changes(new, old, run_id="run_001")
        field_names = [c.field_name for c in changes]
        assert "price" in field_names
        assert "price_uf" in field_names

    def test_deactivation(self):
        old = _make_listing(is_active=True)
        new = _make_listing(is_active=False)
        changes = detect_changes(new, old)
        assert any(c.field_name == "is_active" for c in changes)

    def test_change_metadata(self):
        old = _make_listing(price=8500.0)
        new = _make_listing(price=7900.0)
        changes = detect_changes(new, old, run_id="test_run")
        for ch in changes:
            if ch.field_name == "price":
                assert ch.old_value == 8500.0
                assert ch.new_value == 7900.0
                assert ch.run_id == "test_run"
                break


class TestMergeListing:
    def test_preserves_first_seen(self):
        first_seen = datetime(2024, 1, 1)
        old = _make_listing(first_seen_at=first_seen)
        new = _make_listing(scraped_at=datetime(2024, 2, 1))
        merged = merge_listing(new, old)
        assert merged.first_seen_at == first_seen

    def test_updates_last_seen(self):
        new_scraped = datetime(2024, 3, 15)
        old = _make_listing()
        new = _make_listing(scraped_at=new_scraped)
        merged = merge_listing(new, old)
        assert merged.last_seen_at == new_scraped

    def test_is_active_true(self):
        old = _make_listing(is_active=False)
        new = _make_listing(is_active=True)
        merged = merge_listing(new, old)
        assert merged.is_active is True
