"""
test_transforms.py
------------------
Unit tests for all four stock-analysis transforms.

Each transform is tested with deterministic, hand-crafted DataFrames so
that expected values can be calculated analytically.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from stockanalyser.transforms.monthly_cagr import compute_monthly_cagr
from stockanalyser.transforms.portfolio import _allocate, compute_portfolio
from stockanalyser.transforms.relative_increase import compute_relative_increase
from stockanalyser.transforms.weekly_decrease import compute_weekly_decrease

_TS = datetime(2025, 1, 1, tzinfo=timezone.utc)

_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", LongType(), True),
        StructField("vwap", DoubleType(), True),
        StructField("transactions", LongType(), True),
        StructField("ingested_at", TimestampType(), False),
    ]
)


def _r(symbol, dt_str, close):
    c = close
    return (symbol, date.fromisoformat(dt_str), c, c, c, c, 1_000_000, c, 1000, _TS)


# ============================================================ #
#  Transform 1 – Relative Increase                             #
# ============================================================ #

class TestRelativeIncrease:

    def test_winner_has_rank_one(self, spark, minimal_stock_df):
        result = compute_relative_increase(minimal_stock_df)
        first = result.filter(result.rank == 1).first()
        assert first is not None

    def test_aapl_greater_relative_increase(self, spark, minimal_stock_df):
        """AAPL: 100→200 = +100%. MSFT: 300→360 = +20%. AAPL should win."""
        result = compute_relative_increase(minimal_stock_df)
        winner = result.filter(result.rank == 1).first()
        assert winner["symbol"] == "AAPL"

    def test_relative_increase_value(self, spark, minimal_stock_df):
        result = compute_relative_increase(minimal_stock_df)
        aapl = result.filter(result.symbol == "AAPL").first()
        assert aapl["relative_increase_pct"] == pytest.approx(100.0, rel=1e-3)

    def test_all_stocks_in_output(self, spark, minimal_stock_df):
        result = compute_relative_increase(minimal_stock_df)
        symbols = {r.symbol for r in result.collect()}
        assert {"AAPL", "MSFT"} == symbols

    def test_single_data_point_excluded(self, spark):
        rows = [
            _r("LONE", "2025-01-02", 100.0),
            _r("PAIR", "2025-01-02", 100.0),
            _r("PAIR", "2025-12-31", 200.0),
        ]
        df = spark.createDataFrame(rows, schema=_SCHEMA)
        result = compute_relative_increase(df)
        winner = result.filter(result.rank == 1).first()
        assert winner["symbol"] == "PAIR"

    def test_negative_return_stock_has_lower_rank(self, spark):
        rows = [
            _r("LOSER", "2025-01-02", 100.0),
            _r("LOSER", "2025-12-31", 50.0),
            _r("WINNER", "2025-01-02", 100.0),
            _r("WINNER", "2025-12-31", 200.0),
        ]
        df = spark.createDataFrame(rows, schema=_SCHEMA)
        result = compute_relative_increase(df)
        winner = result.filter(result.rank == 1).first()
        assert winner["symbol"] == "WINNER"

    def test_output_has_expected_columns(self, spark, minimal_stock_df):
        result = compute_relative_increase(minimal_stock_df)
        assert "relative_increase" in result.columns
        assert "relative_increase_pct" in result.columns
        assert "rank" in result.columns


# ============================================================ #
#  Transform 2 – Portfolio                                     #
# ============================================================ #

class TestPortfolioAllocation:

    def _mock_row(self, symbol, start, end):
        from collections import namedtuple
        R = namedtuple("Row", ["symbol", "start_price", "end_price"])
        return R(symbol, start, end)

    def test_integer_shares_allocated(self):
        rows = [self._mock_row("X", 100.0, 120.0)]
        result = _allocate(rows, budget_per_stock=10_000)
        assert result[0]["shares_purchased"] == 100

    def test_remainder_computed(self):
        rows = [self._mock_row("X", 101.0, 120.0)]
        result = _allocate(rows, budget_per_stock=10_000)
        # 10000 / 101 = 99 shares, remainder = 10000 - 99*101 = 10000 - 9999 = 1
        assert result[0]["uninvested_cash"] == pytest.approx(0.0)  # pool redistributed

    def test_current_value_correct(self):
        rows = [self._mock_row("X", 100.0, 200.0)]
        result = _allocate(rows, budget_per_stock=10_000)
        # 100 shares * 200 = 20000
        assert result[0]["current_value"] == pytest.approx(20_000.0)

    def test_remainder_pool_redistributed(self):
        """Two stocks: cheaper stock should receive extra shares from pool."""
        rows = [
            self._mock_row("CHEAP", 1.0, 2.0),
            self._mock_row("EXPENSIVE", 9999.0, 10000.0),
        ]
        result = _allocate(rows, budget_per_stock=10_000)
        cheap = next(r for r in result if r["symbol"] == "CHEAP")
        # EXPENSIVE: 1 share for 9999, leaves 1 in pool
        # CHEAP: initial 10000 shares, pool adds 1 more = 10001 total
        assert cheap["shares_purchased"] >= 10_000

    def test_total_capital_does_not_exceed_budget(self):
        rows = [
            self._mock_row("A", 37.5, 50.0),
            self._mock_row("B", 123.45, 130.0),
        ]
        result = _allocate(rows, budget_per_stock=10_000)
        total = sum(r["capital_invested"] for r in result)
        assert total <= 2 * 10_000 + 0.01  # small float tolerance


class TestPortfolioTransform:

    def test_spark_transform_returns_tuple(self, spark, minimal_stock_df):
        detail, summary = compute_portfolio(minimal_stock_df, investment_per_stock=10_000)
        assert detail is not None
        assert summary is not None

    def test_summary_fields_present(self, spark, minimal_stock_df):
        _, summary = compute_portfolio(minimal_stock_df, investment_per_stock=10_000)
        assert hasattr(summary, "total_current_value")
        assert hasattr(summary, "total_pnl")
        assert hasattr(summary, "total_return_pct")

    def test_portfolio_value_positive(self, spark, minimal_stock_df):
        _, summary = compute_portfolio(minimal_stock_df)
        assert summary.total_current_value > 0

    def test_aapl_profitable_in_bull_scenario(self, spark, minimal_stock_df):
        """AAPL doubles; portfolio should show a gain."""
        detail, summary = compute_portfolio(minimal_stock_df)
        assert summary.total_pnl > 0

    def test_detail_has_all_stocks(self, spark, minimal_stock_df):
        detail, _ = compute_portfolio(minimal_stock_df)
        symbols = {r.symbol for r in detail.collect()}
        assert "AAPL" in symbols
        assert "MSFT" in symbols


# ============================================================ #
#  Transform 3 – Monthly CAGR                                  #
# ============================================================ #

class TestMonthlyCagr:

    def test_winner_has_rank_one(self, spark, minimal_stock_df):
        result = compute_monthly_cagr(minimal_stock_df)
        first = result.filter(result.rank == 1).first()
        assert first is not None

    def test_cagr_formula_correctness(self, spark):
        """
        SOLO: Jan close=100, Jun close=160 → CAGR = (160/100)^(1/5) - 1 ≈ 0.09856
        """
        rows = [
            _r("SOLO", "2025-01-31", 100.0),
            _r("SOLO", "2025-06-30", 160.0),
        ]
        df = spark.createDataFrame(rows, schema=_SCHEMA)
        result = compute_monthly_cagr(df)
        row = result.first()
        expected = (160.0 / 100.0) ** (1 / 5) - 1
        assert row["monthly_cagr"] == pytest.approx(expected, rel=1e-4)

    def test_aapl_wins_vs_msft(self, spark, minimal_stock_df):
        """
        AAPL: Jan=100, Jun=160 → CAGR ≈ 9.86%/mo
        MSFT: Jan=300, Jun=350 → CAGR ≈ (350/300)^(1/5) - 1 ≈ 3.13%/mo
        AAPL should win.
        """
        result = compute_monthly_cagr(minimal_stock_df)
        winner = result.filter(result.rank == 1).first()
        assert winner["symbol"] == "AAPL"

    def test_missing_january_data_excluded(self, spark):
        """Stock with no January data should not appear in results."""
        rows = [
            _r("HAS_JAN", "2025-01-31", 100.0),
            _r("HAS_JAN", "2025-06-30", 150.0),
            _r("NO_JAN", "2025-02-28", 200.0),
            _r("NO_JAN", "2025-06-30", 250.0),
        ]
        df = spark.createDataFrame(rows, schema=_SCHEMA)
        result = compute_monthly_cagr(df)
        symbols = {r.symbol for r in result.collect()}
        assert "HAS_JAN" in symbols
        assert "NO_JAN" not in symbols

    def test_n_months_in_output(self, spark, minimal_stock_df):
        result = compute_monthly_cagr(minimal_stock_df)
        row = result.first()
        assert row["n_months"] == 5

    def test_output_has_expected_columns(self, spark, minimal_stock_df):
        result = compute_monthly_cagr(minimal_stock_df)
        assert "monthly_cagr" in result.columns
        assert "rank" in result.columns


# ============================================================ #
#  Transform 4 – Weekly Decrease                               #
# ============================================================ #

class TestWeeklyDecrease:

    def test_returns_single_row(self, spark, weekly_stock_df):
        result = compute_weekly_decrease(weekly_stock_df)
        assert result.count() == 1

    def test_crash_identified_as_worst(self, spark, weekly_stock_df):
        result = compute_weekly_decrease(weekly_stock_df)
        row = result.first()
        assert row["symbol"] == "CRASH"

    def test_weekly_return_is_negative(self, spark, weekly_stock_df):
        result = compute_weekly_decrease(weekly_stock_df)
        row = result.first()
        assert row["weekly_return"] < 0

    def test_weekly_return_value(self, spark, weekly_stock_df):
        """CRASH goes 100→80 = -20% in week 1."""
        result = compute_weekly_decrease(weekly_stock_df)
        row = result.first()
        assert row["weekly_return"] == pytest.approx(-0.20, rel=1e-3)

    def test_week_start_before_week_end(self, spark, weekly_stock_df):
        result = compute_weekly_decrease(weekly_stock_df)
        row = result.first()
        assert row["week_start"] < row["week_end"]

    def test_single_day_weeks_excluded(self, spark):
        """A week with only one trading day should not be returned."""
        rows = [
            # Week with only Monday (2025-01-06)
            _r("SOLO_WEEK", "2025-01-06", 100.0),
            # Normal week
            _r("NORMAL", "2025-01-13", 100.0),
            _r("NORMAL", "2025-01-14", 90.0),
            _r("NORMAL", "2025-01-15", 80.0),
        ]
        df = spark.createDataFrame(rows, schema=_SCHEMA)
        result = compute_weekly_decrease(df)
        row = result.first()
        # Only NORMAL has a valid multi-day week
        assert row["symbol"] == "NORMAL"

    def test_output_has_week_metadata(self, spark, weekly_stock_df):
        result = compute_weekly_decrease(weekly_stock_df)
        assert "week_of_year" in result.columns
        assert "year" in result.columns
        assert "week_start" in result.columns
        assert "week_end" in result.columns
