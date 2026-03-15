"""
test_validators.py
------------------
Unit tests for StockDataValidator.

Strategy
~~~~~~~~
* Build minimal DataFrames that exercise each validation rule in
  isolation using the raw schema (date as string).
* Assert that valid rows pass through and invalid rows are quarantined
  with the expected failure reason.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from stockanalyser.validation.validators import StockDataValidator

_TS = datetime(2025, 6, 1, tzinfo=timezone.utc)

_RAW_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("vwap", DoubleType(), True),
        StructField("transactions", LongType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)


def _make_raw(spark, rows):
    return spark.createDataFrame(rows, schema=_RAW_SCHEMA)


def _good_row(symbol="AAPL", date="2025-01-02", close=150.0):
    """Returns a perfectly valid row tuple."""
    return (symbol, date, close - 1, close + 1, close - 0.5, close, 1_000_000, close, 1000, _TS)


class TestValidValidator:
    def test_valid_row_passes(self, spark):
        df = _make_raw(spark, [_good_row()])
        result = StockDataValidator().validate(df)
        assert result.total_valid == 1
        assert result.total_quarantine == 0
        assert result.pass_rate == 1.0

    def test_multiple_valid_rows(self, spark):
        rows = [_good_row("AAPL", "2025-01-02", 150.0),
                _good_row("MSFT", "2025-01-02", 300.0)]
        df = _make_raw(spark, rows)
        result = StockDataValidator().validate(df)
        assert result.total_valid == 2

    def test_valid_df_has_date_type(self, spark):
        df = _make_raw(spark, [_good_row()])
        result = StockDataValidator().validate(df)
        from pyspark.sql.types import DateType
        date_field = next(f for f in result.valid.schema.fields if f.name == "date")
        assert isinstance(date_field.dataType, DateType)


class TestNullChecks:
    def test_null_symbol_quarantined(self, spark):
        row = (None, "2025-01-02", 149.0, 151.0, 149.5, 150.0, 1_000_000, 150.0, 1000, _TS)
        df = _make_raw(spark, [row])
        result = StockDataValidator().validate(df)
        assert result.total_quarantine == 1
        assert "non_null_symbol" in result.quarantine_reasons

    def test_null_date_quarantined(self, spark):
        row = ("AAPL", None, 149.0, 151.0, 149.5, 150.0, 1_000_000, 150.0, 1000, _TS)
        df = _make_raw(spark, [row])
        result = StockDataValidator().validate(df)
        assert result.total_quarantine == 1

    def test_null_close_quarantined(self, spark):
        row = ("AAPL", "2025-01-02", 149.0, 151.0, 149.5, None, 1_000_000, 150.0, 1000, _TS)
        df = _make_raw(spark, [row])
        result = StockDataValidator().validate(df)
        assert result.total_quarantine == 1


class TestPriceChecks:
    def test_zero_close_quarantined(self, spark):
        row = ("AAPL", "2025-01-02", 0.0, 0.0, 0.0, 0.0, 0, None, None, _TS)
        df = _make_raw(spark, [row])
        result = StockDataValidator().validate(df)
        assert result.total_quarantine == 1

    def test_negative_close_quarantined(self, spark):
        row = ("AAPL", "2025-01-02", -5.0, -4.0, -6.0, -5.0, 0, None, None, _TS)
        df = _make_raw(spark, [row])
        result = StockDataValidator().validate(df)
        assert result.total_quarantine == 1

    def test_high_lt_low_quarantined(self, spark):
        # high=140 < low=160 is impossible
        row = ("AAPL", "2025-01-02", 150.0, 140.0, 160.0, 150.0, 1_000_000, 150.0, 1000, _TS)
        df = _make_raw(spark, [row])
        result = StockDataValidator().validate(df)
        assert result.total_quarantine == 1

    def test_close_above_high_quarantined(self, spark):
        # close=200 > high=180
        row = ("AAPL", "2025-01-02", 150.0, 180.0, 140.0, 200.0, 1_000_000, 150.0, 1000, _TS)
        df = _make_raw(spark, [row])
        result = StockDataValidator().validate(df)
        assert result.total_quarantine == 1


class TestMixedData:
    def test_mixed_passes_only_valid(self, spark):
        good = _good_row("AAPL", "2025-01-02", 150.0)
        bad = (None, "2025-01-02", 149.0, 151.0, 149.5, 150.0, 1_000_000, 150.0, 1000, _TS)
        df = _make_raw(spark, [good, bad])
        result = StockDataValidator().validate(df)
        assert result.total_valid == 1
        assert result.total_quarantine == 1
        assert result.pass_rate == pytest.approx(0.5)

    def test_quarantine_has_failure_reason_column(self, spark):
        bad = (None, "2025-01-02", 149.0, 151.0, 149.5, 150.0, 1_000_000, 150.0, 1000, _TS)
        df = _make_raw(spark, [bad])
        result = StockDataValidator().validate(df)
        assert "_failure_reason" in result.quarantine.columns
