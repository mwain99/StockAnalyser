"""
portfolio.py
------------
Transform 2: Portfolio simulation.

Rules
~~~~~
* Total capital: $1,000,000
* Allocate $10,000 per company (100 companies → $1,000,000 total).
* Cannot purchase fractional shares.
* Maximise capital deployed:
    - Initial purchase: floor($10,000 / start_price) shares.
    - The fractional remainder per stock is collected as uninvested cash.
    - That remainder pool is then redistributed:  for each stock (sorted
      cheapest first) keep buying one extra share while the pool covers it.
      This ensures the maximum amount of capital is actually invested.
* Final value uses the last available close price for each ticker.
* Dividends are ignored

Fractional-share redistribution detail
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
After the initial integer-share allocation every stock leaves some change.
We collect all that change into a single pool and greedily deploy it: sort stocks
by start_price ascending, buy one additional share for the cheapest stock if the
pool allows, repeat until the pool is exhausted.
"""
from __future__ import annotations

import logging
from typing import NamedTuple

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

from stockanalyser.schemas.stock_schema import PORTFOLIO_SCHEMA

logger = logging.getLogger(__name__)


class PortfolioSummary(NamedTuple):
    total_capital_invested: float
    total_uninvested_cash: float
    total_current_value: float
    total_pnl: float
    total_return_pct: float


def compute_portfolio(
    validated_df: DataFrame,
    investment_per_stock: float = 10_000.0,
) -> tuple[DataFrame, PortfolioSummary]:
    spark: SparkSession = validated_df.sparkSession

    # ---- anchor prices ------------------------------------------------- #
    window_first = Window.partitionBy("symbol").orderBy("date")
    window_last = Window.partitionBy("symbol").orderBy(F.col("date").desc())

    prices = (
        validated_df.select("symbol", "date", "close")
        .withColumn("_rn_first", F.row_number().over(window_first))
        .withColumn("_rn_last", F.row_number().over(window_last))
    )

    start_prices = (
        prices.filter(F.col("_rn_first") == 1)
        .select("symbol", F.col("close").alias("start_price"))
    )
    end_prices = (
        prices.filter(F.col("_rn_last") == 1)
        .select("symbol", F.col("close").alias("end_price"))
    )

    base = start_prices.join(end_prices, on="symbol", how="inner")

    rows: list[Row] = base.collect()
    allocations = _allocate(rows, investment_per_stock)

    detail_df = spark.createDataFrame(allocations, schema=PORTFOLIO_SCHEMA)

    agg = detail_df.agg(
        F.sum("capital_invested").alias("total_capital_invested"),
        F.sum("uninvested_cash").alias("total_uninvested_cash"),
        F.sum("current_value").alias("total_current_value"),
        F.sum("pnl").alias("total_pnl"),
    ).first()

    total_invested = agg["total_capital_invested"]
    total_value = agg["total_current_value"]

    summary = PortfolioSummary(
        total_capital_invested=round(total_invested, 2),
        total_uninvested_cash=round(agg["total_uninvested_cash"], 2),
        total_current_value=round(total_value, 2),
        total_pnl=round(agg["total_pnl"], 2),
        total_return_pct=round(
            (total_value - total_invested) / total_invested * 100
            if total_invested
            else 0.0,
            4,
        ),
    )

    logger.info(
        "Portfolio: invested $%.2f  current value $%.2f  PnL $%.2f (%.2f%%)",
        summary.total_capital_invested,
        summary.total_current_value,
        summary.total_pnl,
        summary.total_return_pct,
    )

    return detail_df, summary

def _allocate(
    rows: list[Row], budget_per_stock: float
) -> list[dict]:
    records: list[dict] = []
    remainder_pool: float = 0.0

    for row in rows:
        start = row["start_price"]
        end = row["end_price"]

        shares = int(budget_per_stock // start)   # floor division
        capital_invested = shares * start
        remainder = budget_per_stock - capital_invested
        remainder_pool += remainder

        records.append(
            {
                "symbol": row["symbol"],
                "start_price": start,
                "end_price": end,
                "shares_purchased": shares,
                "capital_invested": capital_invested,
                "uninvested_cash": remainder,
                "current_value": 0.0,   # filled below
                "pnl": 0.0,             # filled below
            }
        )

    # Redistribute the pool
    records.sort(key=lambda r: r["start_price"])

    for rec in records:
        if remainder_pool <= 0:
            break
        cost = rec["start_price"]
        if remainder_pool >= cost:
            extra_shares = int(remainder_pool // cost)
            extra_cost = extra_shares * cost
            rec["shares_purchased"] += extra_shares
            rec["capital_invested"] += extra_cost
            rec["uninvested_cash"] -= extra_cost
            remainder_pool -= extra_cost

    for rec in records:
        rec["current_value"] = round(rec["shares_purchased"] * rec["end_price"], 4)
        rec["pnl"] = round(rec["current_value"] - rec["capital_invested"], 4)
        rec["capital_invested"] = round(rec["capital_invested"], 4)
        rec["uninvested_cash"] = max(round(rec["uninvested_cash"], 4), 0.0)

    return records