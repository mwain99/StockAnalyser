"""
ingestor.py
-----------
Reads the S&P 500 input file, validates tickers, calls the Polygon API
for every valid ticker concurrently, and returns a raw PySpark DataFrame.

Concurrency & rate limiting
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Polygon's /v2/aggs endpoint is per-ticker only - there is no batch
endpoint. Requests are fired using a ThreadPoolExecutor.

For the free tier (5 req/min) set in local.env:
    API_MAX_WORKERS=1
    API_REQUEST_DELAY=12
"""
from __future__ import annotations

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from stockanalyser.config.settings import Settings, settings as default_settings
from stockanalyser.ingestion.polygon_client import PolygonClient
from stockanalyser.ingestion.ticker_validator import TickerValidator
from stockanalyser.schemas.stock_schema import RAW_STOCK_SCHEMA, SP500_INPUT_SCHEMA

logger = logging.getLogger(__name__)


class StockDataIngestor:
    """
    Orchestrates the full ingestion pipeline:
    1. Read company list from CSV
    2. Validate all ticker symbols
    3. Fetch daily bars from Polygon concurrently for each valid ticker
    4. Return a raw Spark DataFrame
    """

    def __init__(
        self,
        spark: SparkSession,
        client: PolygonClient | None = None,
        validator: TickerValidator | None = None,
        cfg: Settings = default_settings,
        run_validation: bool = False,
    ) -> None:
        self._spark = spark
        self._client = client or PolygonClient(cfg)
        self._validator = validator or TickerValidator(cfg)
        self._cfg = cfg
        self._run_validation = run_validation

    def read_company_list(self, path: str | Path | None = None) -> DataFrame:
        file_path = str(path or self._cfg.input_file)
        logger.info("Reading company list from %s", file_path)
        return (
            self._spark.read.option("header", "true")
            .schema(SP500_INPUT_SCHEMA)
            .csv(file_path)
        )

    def fetch_all(
        self,
        companies_df: DataFrame,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> DataFrame:
        rows = companies_df.select("symbol", "company_name").collect()
        tickers = [row.symbol for row in rows]
        company_names = {row.symbol: row.company_name for row in rows}

        if self._run_validation:
            logger.info("=== TICKER VALIDATION ===")
            validation = self._validator.validate(tickers, company_names=company_names)
            print(validation.report())

            if validation.has_errors:
                logger.warning(
                    "%d invalid tickers will be skipped: %s",
                    len(validation.invalid),
                    ", ".join(validation.invalid),
                )

            valid_tickers = validation.valid
            if not valid_tickers:
                logger.error("No valid tickers to fetch. Aborting.")
                sys.exit(1)
        else:
            logger.info(
                "Ticker validation skipped. Use --validate-tickers to enable."
            )
            valid_tickers = tickers

        logger.info(
            "Fetching %d valid tickers | workers=%d | delay=%.0fs | est. %.1f min",
            len(valid_tickers),
            self._cfg.api_max_workers,
            self._cfg.api_request_delay,
            len(valid_tickers) * self._cfg.api_request_delay / 60,
        )

        ingested_at = datetime.now(timezone.utc)
        records = self._fetch_concurrent(valid_tickers, ingested_at, start_date, end_date)

        if not records:
            logger.warning("No records fetched — returning empty DataFrame.")
            return self._spark.createDataFrame([], schema=RAW_STOCK_SCHEMA)

        return self._spark.createDataFrame(records, schema=RAW_STOCK_SCHEMA)

    def ingest(
        self,
        input_path: str | Path | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> DataFrame:
        companies = self.read_company_list(input_path)
        return self.fetch_all(companies, start_date, end_date)

    def _fetch_concurrent(
        self,
        tickers: list[str],
        ingested_at: datetime,
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict[str, Any]]:
        all_records: list[dict[str, Any]] = []
        failed: list[str] = []

        with ThreadPoolExecutor(max_workers=self._cfg.api_max_workers) as executor:
            future_to_ticker = {
                executor.submit(
                    self._fetch_one, ticker, ingested_at, start_date, end_date
                ): ticker
                for ticker in tickers
            }

            completed = 0
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                completed += 1
                try:
                    records = future.result()
                    all_records.extend(records)
                    logger.info(
                        "[%d/%d] %-6s → %d bars",
                        completed, len(tickers), ticker, len(records),
                    )
                except Exception as exc:
                    failed.append(ticker)
                    logger.error(
                        "[%d/%d] %-6s → FAILED: %s",
                        completed, len(tickers), ticker, exc,
                    )

        if failed:
            logger.warning("Failed tickers (%d): %s", len(failed), ", ".join(failed))

        logger.info("Total records fetched: %d", len(all_records))
        return all_records

    def _fetch_one(
        self,
        ticker: str,
        ingested_at: datetime,
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict[str, Any]]:
        bars = self._client.get_daily_aggregates(ticker, start_date, end_date)
        rows = [self._bar_to_row(ticker, bar, ingested_at) for bar in bars]

        if self._cfg.api_request_delay > 0:
            time.sleep(self._cfg.api_request_delay)

        return rows

    @staticmethod
    def _bar_to_row(
        ticker: str, bar: dict[str, Any], ingested_at: datetime
    ) -> dict[str, Any]:
        ts_ms: int = bar.get("t", 0)
        trade_date = datetime.fromtimestamp(ts_ms / 1_000, tz=timezone.utc).date().isoformat()
        return {
            "symbol": ticker,
            "date": trade_date,
            "open": float(bar.get("o") or 0),
            "high": float(bar.get("h") or 0),
            "low": float(bar.get("l") or 0),
            "close": float(bar.get("c") or 0),
            "volume": int(bar.get("v") or 0),
            "vwap": float(bar.get("vw") or 0) if bar.get("vw") else None,
            "transactions": int(bar.get("n") or 0) if bar.get("n") else None,
            "ingested_at": ingested_at,
        }