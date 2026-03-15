"""
test_pipeline_integration.py
-----------------------------
Integration test: runs the full pipeline (ingest → validate → all transforms)
against a synthetic dataset injected via a mocked PolygonClient.

This test exercises the wiring between all stages without making real HTTP
calls or writing to production paths.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ============================================================ #
#  Test data factory                                      #
# ============================================================ #

def _ts_ms(year, month, day) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


def _make_bars(start_price: float, end_price: float) -> list[dict]:
    """
    Generate a daily bar sequence with 12 monthly data points spread across
    2025, linearly interpolated between start_price and end_price.
    Months 1 (January) and 6 (June) are included for CAGR tests.
    """
    monthly_dates = [
        (1, 31), (2, 28), (3, 31), (4, 30), (5, 31), (6, 30),
        (7, 31), (8, 31), (9, 30), (10, 31), (11, 30), (12, 31),
    ]
    n = len(monthly_dates)
    bars = []
    for i, (month, day) in enumerate(monthly_dates):
        price = start_price + (end_price - start_price) * i / (n - 1)
        ts = _ts_ms(2025, month, day)
        bars.append({"t": ts, "o": price, "h": price, "l": price, "c": price,
                     "v": 1_000_000, "vw": price, "n": 5000})

    # Add daily data for two weeks in January for the weekly-decrease test
    # Week of Jan 6-10: WINNER goes up; LOSER crashes
    return bars


def _make_loser_bars() -> list[dict]:
    """Same as _make_bars but with a deep weekly crash in January."""
    bars = _make_bars(100.0, 200.0)
    # Override Jan data with a crash week
    crash_days = [(1, 6, 100.0), (1, 7, 90.0), (1, 8, 82.0),
                  (1, 9, 81.0), (1, 10, 80.0)]
    for m, d, c in crash_days:
        bars.append({"t": _ts_ms(2025, m, d), "o": c, "h": c, "l": c,
                     "c": c, "v": 1_000_000, "vw": c, "n": 5000})
    return bars


# ============================================================ #
#  Test company list                                      #
# ============================================================ #

COMPANIES_CSV = "name,symbol\nApple Inc,AAPL\nMicrosoft Corp,MSFT\nCrash Corp,CCRP\n"

BARS_BY_TICKER = {
    "AAPL": _make_bars(100.0, 250.0),   # +150% – biggest relative gain
    "MSFT": _make_bars(300.0, 360.0),   # +20%
    "CCRP": _make_loser_bars(),          # weekly crash in Jan
}

class TestFullPipeline:

    @pytest.fixture()
    def pipeline_result(self, spark, tmp_path):
        import sys
        sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

        csv_path = tmp_path / "companies.csv"
        csv_path.write_text(COMPANIES_CSV)

        mock_client = MagicMock()
        mock_client.get_daily_aggregates.side_effect = (
            lambda ticker, *a, **kw: BARS_BY_TICKER.get(ticker.upper(), [])
        )

        with patch.dict(os.environ, {"API_KEY": "integration_test_key"}):
            from stockanalyser.config.settings import Settings
            from stockanalyser.ingestion.ingestor import StockDataIngestor
            from stockanalyser.transforms.monthly_cagr import compute_monthly_cagr
            from stockanalyser.transforms.portfolio import compute_portfolio
            from stockanalyser.transforms.relative_increase import compute_relative_increase
            from stockanalyser.transforms.weekly_decrease import compute_weekly_decrease
            from stockanalyser.validation.validators import StockDataValidator

            cfg = Settings()
            ingestor = StockDataIngestor(spark, client=mock_client, cfg=cfg)
            raw_df = ingestor.ingest(input_path=str(csv_path))

            validator = StockDataValidator()
            val_result = validator.validate(raw_df)
            valid_df = val_result.valid.cache()

            t1 = compute_relative_increase(valid_df)
            t2_detail, t2_summary = compute_portfolio(valid_df, investment_per_stock=10_000)
            t3 = compute_monthly_cagr(valid_df)
            t4 = compute_weekly_decrease(valid_df)

        return {
            "raw_count": raw_df.count(),
            "valid_count": val_result.total_valid,
            "t1": t1,
            "t2_detail": t2_detail,
            "t2_summary": t2_summary,
            "t3": t3,
            "t4": t4,
        }

    def test_ingestion_produced_records(self, pipeline_result):
        assert pipeline_result["raw_count"] > 0

    def test_validation_passes_all(self, pipeline_result):
        assert pipeline_result["valid_count"] == pipeline_result["raw_count"]

    def test_t1_winner_is_aapl(self, pipeline_result):
        winner = pipeline_result["t1"].filter(pipeline_result["t1"].rank == 1).first()
        assert winner["symbol"] == "AAPL"

    def test_t1_aapl_return_approx_150pct(self, pipeline_result):
        aapl = pipeline_result["t1"].filter(
            pipeline_result["t1"].symbol == "AAPL"
        ).first()
        assert aapl["relative_increase_pct"] == pytest.approx(150.0, rel=1e-2)

    def test_t2_portfolio_value_positive(self, pipeline_result):
        assert pipeline_result["t2_summary"].total_current_value > 0

    def test_t2_all_stocks_in_detail(self, pipeline_result):
        symbols = {r.symbol for r in pipeline_result["t2_detail"].collect()}
        assert {"AAPL", "MSFT", "CCRP"}.issubset(symbols)

    def test_t3_returns_cagr_result(self, pipeline_result):
        assert pipeline_result["t3"].count() > 0
        row = pipeline_result["t3"].filter(
            pipeline_result["t3"].rank == 1
        ).first()
        assert row is not None
        assert row["monthly_cagr"] > 0

    def test_t4_identifies_crash_week(self, pipeline_result):
        row = pipeline_result["t4"].first()
        assert row is not None
        assert row["symbol"] == "CCRP"
        assert row["weekly_return"] < 0
