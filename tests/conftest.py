"""
conftest.py
-----------
Shared pytest fixtures.

All tests import SparkSession from here so that only one JVM is started
for the entire test suite.  The session is scoped to the session level
(re-used across all test files, torn down at the end).
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Generator

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("API_KEY", "test_key")

@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Single SparkSession re-used across the whole test session."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("stockanalyser-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# --------------------------------------------------------------------------- #
#  Shared schema for test DataFrames                                            #
# --------------------------------------------------------------------------- #

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_VALIDATED_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("date", DateType(), nullable=False),
        StructField("open", DoubleType(), nullable=False),
        StructField("high", DoubleType(), nullable=False),
        StructField("low", DoubleType(), nullable=False),
        StructField("close", DoubleType(), nullable=False),
        StructField("volume", LongType(), nullable=True),
        StructField("vwap", DoubleType(), nullable=True),
        StructField("transactions", LongType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)

_TS = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _row(symbol, dt, close, open_=None, high=None, low=None):
    c = close
    o = open_ or close
    h = high or close
    l = low or close
    return (symbol, date.fromisoformat(dt), o, h, l, c, 1_000_000, c, 1000, _TS)


@pytest.fixture()
def minimal_stock_df(spark):
    """
    Two stocks (AAPL, MSFT) with 12 monthly data points each (2025).
    AAPL: steady rise 100 → 200
    MSFT: steady rise 300 → 360
    """
    rows = [
        # Monthly closes (approx end-of-month)
        _row("AAPL", "2025-01-31", 100.0),
        _row("AAPL", "2025-02-28", 110.0),
        _row("AAPL", "2025-03-31", 120.0),
        _row("AAPL", "2025-04-30", 130.0),
        _row("AAPL", "2025-05-31", 150.0),
        _row("AAPL", "2025-06-30", 160.0),
        _row("AAPL", "2025-07-31", 170.0),
        _row("AAPL", "2025-08-31", 180.0),
        _row("AAPL", "2025-09-30", 185.0),
        _row("AAPL", "2025-10-31", 190.0),
        _row("AAPL", "2025-11-30", 195.0),
        _row("AAPL", "2025-12-31", 200.0),
        _row("MSFT", "2025-01-31", 300.0),
        _row("MSFT", "2025-02-28", 310.0),
        _row("MSFT", "2025-03-31", 320.0),
        _row("MSFT", "2025-04-30", 330.0),
        _row("MSFT", "2025-05-31", 340.0),
        _row("MSFT", "2025-06-30", 350.0),
        _row("MSFT", "2025-07-31", 355.0),
        _row("MSFT", "2025-08-31", 358.0),
        _row("MSFT", "2025-09-30", 356.0),
        _row("MSFT", "2025-10-31", 358.0),
        _row("MSFT", "2025-11-30", 359.0),
        _row("MSFT", "2025-12-31", 360.0),
    ]
    return spark.createDataFrame(rows, schema=_VALIDATED_SCHEMA)


@pytest.fixture()
def weekly_stock_df(spark):
    """
    Two stocks with daily data across two weeks so we can test weekly
    decrease.  CRASH has a -20% week; STABLE is flat.
    """
    rows = [
        # Week 1 (Jan 6-10 2025) – CRASH drops hard
        _row("CRASH", "2025-01-06", 100.0),
        _row("CRASH", "2025-01-07", 90.0),
        _row("CRASH", "2025-01-08", 82.0),
        _row("CRASH", "2025-01-09", 81.0),
        _row("CRASH", "2025-01-10", 80.0),   # -20%
        # Week 1 (Jan 6-10 2025) – STABLE stays put
        _row("STABLE", "2025-01-06", 50.0),
        _row("STABLE", "2025-01-07", 50.5),
        _row("STABLE", "2025-01-08", 50.2),
        _row("STABLE", "2025-01-09", 50.4),
        _row("STABLE", "2025-01-10", 50.0),
        # Week 2 (Jan 13-17 2025) – CRASH recovers slightly
        _row("CRASH", "2025-01-13", 81.0),
        _row("CRASH", "2025-01-14", 83.0),
        _row("CRASH", "2025-01-15", 85.0),
        _row("CRASH", "2025-01-16", 86.0),
        _row("CRASH", "2025-01-17", 87.0),   # +8.75%
        _row("STABLE", "2025-01-13", 50.0),
        _row("STABLE", "2025-01-14", 50.0),
        _row("STABLE", "2025-01-15", 50.0),
        _row("STABLE", "2025-01-16", 50.0),
        _row("STABLE", "2025-01-17", 50.0),
    ]
    return spark.createDataFrame(rows, schema=_VALIDATED_SCHEMA)
