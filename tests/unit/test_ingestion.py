"""
test_ingestion.py
-----------------
Unit tests for the ingestion layer.

Strategy
~~~~~~~~
* The HTTP layer is mocked using the ``responses`` library so that no
  real network calls are made.
* PolygonClient is tested for correct URL construction, header injection,
  response parsing, and error handling.
* StockDataIngestor is tested via a mock PolygonClient so we verify the
  wiring logic (ticker extraction, bar-to-row mapping, empty-response
  handling) without depending on the HTTP client.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import responses as resp_lib

from stockanalyser.ingestion.ingestor import StockDataIngestor
from stockanalyser.ingestion.polygon_client import PolygonClient


# ============================================================ #
#  Helpers / fixtures                                          #
# ============================================================ #

def _polygon_response(ticker: str, bars: list[dict]) -> dict:
    return {
        "ticker": ticker,
        "status": "OK",
        "resultsCount": len(bars),
        "results": bars,
    }


def _bar(ts_ms: int, o=100.0, h=105.0, l=99.0, c=102.0, v=1_000_000, vw=101.5, n=5000):
    return {"t": ts_ms, "o": o, "h": h, "l": l, "c": c, "v": v, "vw": vw, "n": n}


_JAN2_MS = int(datetime(2025, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
_JAN3_MS = int(datetime(2025, 1, 3, tzinfo=timezone.utc).timestamp() * 1000)

class TestPolygonClientURLConstruction:

    @resp_lib.activate
    def test_correct_url_called(self):
        expected_url_prefix = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2025-01-01/2025-12-31"
        resp_lib.add(
            resp_lib.GET,
            expected_url_prefix,
            json=_polygon_response("AAPL", [_bar(_JAN2_MS)]),
            status=200,
            match_querystring=False,
        )
        with patch.dict("os.environ", {"API_KEY": "test123"}):
            from stockanalyser.config.settings import Settings
            cfg = Settings()
            client = PolygonClient(cfg)
            result = client.get_daily_aggregates("AAPL")

        assert len(result) == 1

    @resp_lib.activate
    def test_ticker_uppercased(self):
        resp_lib.add(
            resp_lib.GET,
            "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2025-01-01/2025-12-31",
            json=_polygon_response("AAPL", []),
            status=200,
            match_querystring=False,
        )
        with patch.dict("os.environ", {"API_KEY": "test123"}):
            from stockanalyser.config.settings import Settings
            cfg = Settings()
            client = PolygonClient(cfg)
            client.get_daily_aggregates("aapl")   # lowercase input

    @resp_lib.activate
    def test_bearer_token_in_header(self):
        resp_lib.add(
            resp_lib.GET,
            "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2025-01-01/2025-12-31",
            json=_polygon_response("AAPL", []),
            status=200,
            match_querystring=False,
        )
        with patch.dict("os.environ", {"API_KEY": "my_secret_key"}):
            from stockanalyser.config.settings import Settings
            cfg = Settings()
            client = PolygonClient(cfg)
            client.get_daily_aggregates("AAPL")

        sent_headers = resp_lib.calls[0].request.headers
        assert sent_headers["Authorization"] == "Bearer my_secret_key"


class TestPolygonClientResponseParsing:

    @resp_lib.activate
    def test_returns_results_list(self):
        bars = [_bar(_JAN2_MS, c=150.0), _bar(_JAN3_MS, c=152.0)]
        resp_lib.add(
            resp_lib.GET,
            "https://api.polygon.io/v2/aggs/ticker/TSLA/range/1/day/2025-01-01/2025-12-31",
            json=_polygon_response("TSLA", bars),
            status=200,
            match_querystring=False,
        )
        with patch.dict("os.environ", {"API_KEY": "x"}):
            from stockanalyser.config.settings import Settings
            client = PolygonClient(Settings())
            result = client.get_daily_aggregates("TSLA")

        assert len(result) == 2
        assert result[0]["c"] == 150.0

    @resp_lib.activate
    def test_empty_results_returns_empty_list(self):
        resp_lib.add(
            resp_lib.GET,
            "https://api.polygon.io/v2/aggs/ticker/NODATA/range/1/day/2025-01-01/2025-12-31",
            json={"ticker": "NODATA", "status": "OK", "resultsCount": 0, "results": []},
            status=200,
            match_querystring=False,
        )
        with patch.dict("os.environ", {"API_KEY": "x"}):
            from stockanalyser.config.settings import Settings
            client = PolygonClient(Settings())
            result = client.get_daily_aggregates("NODATA")

        assert result == []

    @resp_lib.activate
    def test_error_status_returns_empty_list(self):
        resp_lib.add(
            resp_lib.GET,
            "https://api.polygon.io/v2/aggs/ticker/ERR/range/1/day/2025-01-01/2025-12-31",
            json={"status": "ERROR", "error": "Not found"},
            status=200,
            match_querystring=False,
        )
        with patch.dict("os.environ", {"API_KEY": "x"}):
            from stockanalyser.config.settings import Settings
            client = PolygonClient(Settings())
            result = client.get_daily_aggregates("ERR")

        assert result == []

    @resp_lib.activate
    def test_http_500_raises(self):
        resp_lib.add(
            resp_lib.GET,
            "https://api.polygon.io/v2/aggs/ticker/FAIL/range/1/day/2025-01-01/2025-12-31",
            status=500,
            match_querystring=False,
        )
        with patch.dict("os.environ", {"API_KEY": "x", "API_MAX_RETRIES": "0"}):
            from stockanalyser.config.settings import Settings
            client = PolygonClient(Settings())
            with pytest.raises(Exception):
                client.get_daily_aggregates("FAIL")


# ============================================================ #
#  StockDataIngestor tests (mock client)                       #
# ============================================================ #

class TestStockDataIngestor:

    def _mock_client(self, bars_by_ticker: dict) -> MagicMock:
        client = MagicMock()
        client.get_daily_aggregates.side_effect = (
            lambda ticker, *a, **kw: bars_by_ticker.get(ticker.upper(), [])
        )
        return client

    def test_ingestor_produces_dataframe(self, spark, tmp_path):
        csv = tmp_path / "companies.csv"
        csv.write_text("name,symbol\nApple Inc,AAPL\n")

        client = self._mock_client({
            "AAPL": [_bar(_JAN2_MS, c=150.0)],
        })
        with patch.dict("os.environ", {"API_KEY": "x"}):
            from stockanalyser.config.settings import Settings
            ingestor = StockDataIngestor(spark, client=client, cfg=Settings())
            df = ingestor.ingest(input_path=str(csv))

        assert df.count() == 1
        row = df.first()
        assert row["symbol"] == "AAPL"
        assert row["close"] == pytest.approx(102.0)   # from _bar default c=102.0

    def test_date_correctly_parsed(self, spark, tmp_path):
        csv = tmp_path / "companies.csv"
        csv.write_text("name,symbol\nApple Inc,AAPL\n")

        client = self._mock_client({
            "AAPL": [_bar(_JAN2_MS)],
        })
        with patch.dict("os.environ", {"API_KEY": "x"}):
            from stockanalyser.config.settings import Settings
            ingestor = StockDataIngestor(spark, client=client, cfg=Settings())
            df = ingestor.ingest(input_path=str(csv))

        row = df.first()
        # date should be a string "2025-01-02" in the raw layer
        assert "2025-01-02" in row["date"]

    def test_failed_ticker_skipped(self, spark, tmp_path):
        csv = tmp_path / "companies.csv"
        csv.write_text("name,symbol\nApple Inc,AAPL\nBad Corp,BAAD\n")

        client = MagicMock()
        client.get_daily_aggregates.side_effect = (
            lambda ticker, *a, **kw: (
                [_bar(_JAN2_MS)] if ticker == "AAPL" else (_ for _ in ()).throw(
                    RuntimeError("API error")
                )
            )
        )
        with patch.dict("os.environ", {"API_KEY": "x"}):
            from stockanalyser.config.settings import Settings
            ingestor = StockDataIngestor(spark, client=client, cfg=Settings())
            df = ingestor.ingest(input_path=str(csv))

        # Only AAPL data survived
        assert df.count() == 1
        assert df.first()["symbol"] == "AAPL"

    def test_all_failed_returns_empty_df(self, spark, tmp_path):
        csv = tmp_path / "companies.csv"
        csv.write_text("name,symbol\nApple Inc,AAPL\n")

        client = self._mock_client({"AAPL": []})
        with patch.dict("os.environ", {"API_KEY": "x"}):
            from stockanalyser.config.settings import Settings
            ingestor = StockDataIngestor(spark, client=client, cfg=Settings())
            df = ingestor.ingest(input_path=str(csv))

        assert df.count() == 0

    def test_multiple_tickers(self, spark, tmp_path):
        csv = tmp_path / "companies.csv"
        csv.write_text("name,symbol\nApple Inc,AAPL\nMicrosoft,MSFT\n")

        client = self._mock_client({
            "AAPL": [_bar(_JAN2_MS, c=150.0), _bar(_JAN3_MS, c=151.0)],
            "MSFT": [_bar(_JAN2_MS, c=300.0)],
        })
        with patch.dict("os.environ", {"API_KEY": "x"}):
            from stockanalyser.config.settings import Settings
            ingestor = StockDataIngestor(spark, client=client, cfg=Settings())
            df = ingestor.ingest(input_path=str(csv))

        assert df.count() == 3
        symbols = {r.symbol for r in df.collect()}
        assert symbols == {"AAPL", "MSFT"}
