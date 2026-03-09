"""Unit tests for Chile-specific normalization functions."""
from __future__ import annotations

import pytest

from src.normalization import (
    generate_listing_fingerprint,
    normalize_condition,
    normalize_property_type,
    normalize_region,
    parse_area_m2,
    parse_bedrooms_bathrooms,
    parse_expenses,
    parse_location_components,
    parse_price,
    parse_room_attributes,
)


# ── parse_price ───────────────────────────────────────────────────────────────

class TestParsePrice:
    def test_uf_with_dots(self):
        val, cur, is_from = parse_price("UF 8.500")
        assert val == 8500.0
        assert cur == "UF"
        assert is_from is False

    def test_uf_uppercase(self):
        val, cur, _ = parse_price("UF 4.500")
        assert val == 4500.0
        assert cur == "UF"

    def test_clp_with_dots(self):
        val, cur, _ = parse_price("$ 45.000.000")
        assert val == 45_000_000.0
        assert cur == "CLP"

    def test_clp_small(self):
        val, cur, _ = parse_price("$ 85.000.000")
        assert val == 85_000_000.0
        assert cur == "CLP"

    def test_desde_prefix(self):
        val, cur, is_from = parse_price("Desde UF 3.200")
        assert val == 3200.0
        assert cur == "UF"
        assert is_from is True

    def test_none_input(self):
        val, cur, is_from = parse_price(None)
        assert val is None
        assert cur is None
        assert is_from is False

    def test_empty_string(self):
        val, cur, is_from = parse_price("")
        assert val is None

    def test_clf_is_uf(self):
        val, cur, _ = parse_price("CLF 4.500")
        assert cur == "UF"

    def test_maintenance_fee_clp(self):
        val, cur, _ = parse_price("$ 120.000")
        assert val == 120_000.0
        assert cur == "CLP"


# ── parse_area_m2 ─────────────────────────────────────────────────────────────

class TestParseAreaM2:
    def test_m2_basic(self):
        assert parse_area_m2("85 m²") == 85.0

    def test_m2_with_dots(self):
        assert parse_area_m2("1.200 m²") == 1200.0

    def test_mt2_alias(self):
        assert parse_area_m2("120 mt2") == 120.0

    def test_ha_conversion(self):
        result = parse_area_m2("0,5 ha")
        assert result == 5000.0

    def test_hectarea_full(self):
        result = parse_area_m2("1 hectárea")
        assert result == 10_000.0

    def test_with_label_prefix(self):
        assert parse_area_m2("Total: 95 m²") == 95.0

    def test_none_input(self):
        assert parse_area_m2(None) is None

    def test_no_unit(self):
        # bare number fallback
        result = parse_area_m2("85")
        assert result == 85.0


# ── parse_bedrooms_bathrooms ──────────────────────────────────────────────────

class TestParseBedrooms:
    def test_dormitorios(self):
        result = parse_bedrooms_bathrooms("3 Dormitorios")
        assert result == {"bedrooms": 3}

    def test_banos(self):
        result = parse_bedrooms_bathrooms("2 Baños")
        assert result == {"bathrooms": 2}

    def test_estacionamiento(self):
        result = parse_bedrooms_bathrooms("1 Estacionamiento")
        assert result == {"parking_spaces": 1}

    def test_bodega(self):
        result = parse_bedrooms_bathrooms("1 Bodega")
        assert result == {"storage_room": True}

    def test_empty(self):
        result = parse_bedrooms_bathrooms("")
        assert result == {}

    def test_no_number(self):
        result = parse_bedrooms_bathrooms("Dormitorios")
        assert result == {}  # No number to extract


class TestParseRoomAttributes:
    def test_aggregate(self):
        result = parse_room_attributes(["3 Dormitorios", "2 Baños", "85 m²", "1 Estacionamiento"])
        assert result["bedrooms"] == 3
        assert result["bathrooms"] == 2
        assert result["parking_spaces"] == 1

    def test_empty_list(self):
        assert parse_room_attributes([]) == {}


# ── parse_location_components ─────────────────────────────────────────────────

class TestParseLocation:
    def test_commune_region(self):
        region, commune, neighborhood = parse_location_components(
            "Las Condes, Región Metropolitana"
        )
        assert "Metropolitana" in region
        assert commune == "Las Condes"

    def test_three_parts(self):
        region, commune, neighborhood = parse_location_components(
            "Reñaca, Viña del Mar, Valparaíso"
        )
        assert "Valparaíso" in region
        assert commune == "Viña del Mar"
        assert neighborhood == "Reñaca"

    def test_none(self):
        region, commune, neighborhood = parse_location_components(None)
        assert all(x is None for x in (region, commune, neighborhood))

    def test_only_commune(self):
        region, commune, neighborhood = parse_location_components("Las Condes")
        assert commune == "Las Condes"


# ── parse_expenses ────────────────────────────────────────────────────────────

class TestParseExpenses:
    def test_clp_fee(self):
        val, cur = parse_expenses("$ 120.000 gastos comunes")
        assert val == 120_000.0
        assert cur == "CLP"

    def test_none(self):
        val, cur = parse_expenses(None)
        assert val is None

    def test_uf_fee(self):
        val, cur = parse_expenses("UF 2,5 expensas")
        assert val == 2.5
        assert cur == "UF"


# ── normalize_condition ───────────────────────────────────────────────────────

class TestNormalizeCondition:
    def test_new(self):
        assert normalize_condition("Nuevo") == "new"
        assert normalize_condition("En estreno") == "new"

    def test_used(self):
        assert normalize_condition("Usado") == "used"
        assert normalize_condition("Segunda mano") == "used"

    def test_project(self):
        assert normalize_condition("Proyecto") == "project"
        assert normalize_condition("En construcción") == "project"

    def test_none(self):
        assert normalize_condition(None) is None


# ── normalize_property_type ───────────────────────────────────────────────────

class TestNormalizePropertyType:
    def test_departamento(self):
        assert normalize_property_type("Departamento") == "apartment"
        assert normalize_property_type("Depto") == "apartment"

    def test_casa(self):
        assert normalize_property_type("Casa") == "house"

    def test_terreno(self):
        assert normalize_property_type("Terreno") == "land"
        assert normalize_property_type("Parcela") == "land"

    def test_oficina(self):
        assert normalize_property_type("Oficina") == "office"

    def test_none(self):
        assert normalize_property_type(None) is None


# ── generate_listing_fingerprint ─────────────────────────────────────────────

class TestFingerprint:
    def test_deterministic(self):
        fp1 = generate_listing_fingerprint("MLC-123", 8500.0, 3, 85.0, "Las Condes")
        fp2 = generate_listing_fingerprint("MLC-123", 8500.0, 3, 85.0, "Las Condes")
        assert fp1 == fp2

    def test_different_on_price_change(self):
        fp1 = generate_listing_fingerprint("MLC-123", 8500.0, 3, 85.0, "Las Condes")
        fp2 = generate_listing_fingerprint("MLC-123", 9000.0, 3, 85.0, "Las Condes")
        assert fp1 != fp2

    def test_is_string(self):
        fp = generate_listing_fingerprint("MLC-123", None, None, None, None)
        assert isinstance(fp, str)
        assert len(fp) > 0
