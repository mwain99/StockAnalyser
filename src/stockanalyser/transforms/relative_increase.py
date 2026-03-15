"""
relative_increase.py
--------------------
Transform 1: Which stock had the greatest relative increase in price
over the full analysis period?

Relative increase = (end_price - start_price) / start_price

*  "start_price" = closing price on the earliest available trading date
   for each ticker within the configured date range.
*  "end_price"   = closing price on the latest available trading date.
*  Stocks with a single data point are excluded.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def compute_relative_increase(validated_df: DataFrame) -> DataFrame:
    window_by_symbol = Window.partitionBy("symbol").orderBy("date")

    anchored = (
        validated_df.select("symbol", "date", "close")
        .withColumn("row_first", F.row_number().over(window_by_symbol))
        .withColumn(
            "row_last",
            F.row_number().over(
                Window.partitionBy("symbol").orderBy(F.col("date").desc())
            ),
        )
    )

    first_prices = (
        anchored.filter(F.col("row_first") == 1)
        .select(F.col("symbol"), F.col("close").alias("start_price"))
    )
    last_prices = (
        anchored.filter(F.col("row_last") == 1)
        .select(F.col("symbol"), F.col("close").alias("end_price"))
    )

    result = (
        first_prices.join(last_prices, on="symbol", how="inner")
        .filter(F.col("start_price") > 0)
        .withColumn(
            "relative_increase",
            (F.col("end_price") - F.col("start_price")) / F.col("start_price"),
        )
        .withColumn(
            "relative_increase_pct",
            F.round(F.col("relative_increase") * 100, 4),
        )
        .withColumn(
            "rank",
            F.rank().over(
                Window.orderBy(F.col("relative_increase").desc())
            ).cast("integer"),
        )
        .orderBy("rank")
    )

    winner = result.filter(F.col("rank") == 1).first()
    if winner:
        logger.info(
            "Greatest relative increase: %s  +%.2f%%",
            winner["symbol"],
            winner["relative_increase_pct"],
        )

    return result
