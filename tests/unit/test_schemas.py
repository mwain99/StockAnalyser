"""
test_schemas.py
---------------
Unit tests for the schema definitions in stockanalyser.schemas.

Tests verify:
* All required schemas are importable and are StructType instances.
* Each schema contains the expected columns with the correct types.
* Nullable constraints are honoured.
"""
from __future__ import annotations

import pytest
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from stockanalyser.schemas.stock_schema import (
    MONTHLY_CAGR_SCHEMA,
    PORTFOLIO_SCHEMA,
    PORTFOLIO_SUMMARY_SCHEMA,
    RAW_STOCK_SCHEMA,
    RELATIVE_INCREASE_SCHEMA,
    SP500_INPUT_SCHEMA,
    VALIDATED_STOCK_SCHEMA,
    WEEKLY_DECREASE_SCHEMA,
)


def _field_map(schema):
    return {f.name: f for f in schema.fields}


class TestSP500InputSchema:
    def test_has_name_column(self):
        fm = _field_map(SP500_INPUT_SCHEMA)
        assert "name" in fm
        assert isinstance(fm["name"].dataType, StringType)

    def test_has_symbol_column(self):
        fm = _field_map(SP500_INPUT_SCHEMA)
        assert "symbol" in fm
        assert isinstance(fm["symbol"].dataType, StringType)

    def test_both_not_nullable(self):
        fm = _field_map(SP500_INPUT_SCHEMA)
        assert not fm["name"].nullable
        assert not fm["symbol"].nullable


class TestRawStockSchema:
    def test_required_columns_present(self):
        names = {f.name for f in RAW_STOCK_SCHEMA.fields}
        assert {"symbol", "date", "open", "high", "low", "close"}.issubset(names)

    def test_date_is_string(self):
        fm = _field_map(RAW_STOCK_SCHEMA)
        assert isinstance(fm["date"].dataType, StringType)

    def test_close_is_double(self):
        fm = _field_map(RAW_STOCK_SCHEMA)
        assert isinstance(fm["close"].dataType, DoubleType)

    def test_ingested_at_is_timestamp(self):
        fm = _field_map(RAW_STOCK_SCHEMA)
        assert isinstance(fm["ingested_at"].dataType, TimestampType)

    def test_symbol_not_nullable(self):
        fm = _field_map(RAW_STOCK_SCHEMA)
        assert not fm["symbol"].nullable


class TestValidatedStockSchema:
    def test_date_is_date_type(self):
        """Validated schema promotes date string → DateType."""
        fm = _field_map(VALIDATED_STOCK_SCHEMA)
        assert isinstance(fm["date"].dataType, DateType)

    def test_ohlc_not_nullable(self):
        fm = _field_map(VALIDATED_STOCK_SCHEMA)
        for col in ("open", "high", "low", "close"):
            assert not fm[col].nullable, f"{col} should be non-nullable"


class TestRelativeIncreaseSchema:
    def test_has_rank_column(self):
        fm = _field_map(RELATIVE_INCREASE_SCHEMA)
        assert "rank" in fm
        assert isinstance(fm["rank"].dataType, IntegerType)

    def test_has_relative_increase_pct(self):
        fm = _field_map(RELATIVE_INCREASE_SCHEMA)
        assert "relative_increase_pct" in fm


class TestPortfolioSchema:
    def test_shares_purchased_is_long(self):
        fm = _field_map(PORTFOLIO_SCHEMA)
        assert isinstance(fm["shares_purchased"].dataType, LongType)

    def test_pnl_is_double(self):
        fm = _field_map(PORTFOLIO_SCHEMA)
        assert isinstance(fm["pnl"].dataType, DoubleType)


class TestMonthlyCagrSchema:
    def test_n_months_is_integer(self):
        fm = _field_map(MONTHLY_CAGR_SCHEMA)
        assert isinstance(fm["n_months"].dataType, IntegerType)

    def test_monthly_cagr_is_double(self):
        fm = _field_map(MONTHLY_CAGR_SCHEMA)
        assert isinstance(fm["monthly_cagr"].dataType, DoubleType)


class TestWeeklyDecreaseSchema:
    def test_has_week_of_year(self):
        fm = _field_map(WEEKLY_DECREASE_SCHEMA)
        assert "week_of_year" in fm
        assert isinstance(fm["week_of_year"].dataType, IntegerType)

    def test_week_start_is_date(self):
        fm = _field_map(WEEKLY_DECREASE_SCHEMA)
        assert isinstance(fm["week_start"].dataType, DateType)
