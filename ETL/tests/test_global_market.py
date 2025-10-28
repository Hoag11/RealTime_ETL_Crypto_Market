"""Unit tests for the global market batch ingestion helpers."""

import pytest

from scripts.batch.global_market import validate_global_market_payload


def test_validate_global_market_payload_success():
    payload = {
        "data": {
            "active_cryptocurrencies": 100,
            "markets": 120,
            "total_market_cap": {"usd": 1_000_000_000},
            "extra": "value",
        }
    }

    validated = validate_global_market_payload(payload)

    assert validated["total_market_cap"]["usd"] == 1_000_000_000


def test_validate_global_market_payload_missing_required_section():
    payload = {
        "data": {
            "active_cryptocurrencies": 100,
            # missing total_market_cap
            "markets": 120,
        }
    }

    with pytest.raises(ValueError):
        validate_global_market_payload(payload)


def test_validate_global_market_payload_invalid_type():
    with pytest.raises(ValueError):
        validate_global_market_payload(["not", "a", "dict"])
