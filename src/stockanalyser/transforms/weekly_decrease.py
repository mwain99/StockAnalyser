"""
weekly_decrease.py
------------------
Transform 4: Which stock had the greatest decrease in value within a
single calendar week, and which week was it?

Week definition
~~~~~~~~~~~~~~~
ISO week (Monday–Sunday). We group by the Monday date of each week
rather than (year, weekofyear) to avoid a Spark issue at year
boundaries as dates like Dec 29-31 have weekofyear=1 but year=prev year,
so grouping by (year, weekofyear) incorrectly merges late-December dates
with early-January dates into one fake week spanning the whole year.

Weekly return = (week_end_price - week_start_price) / week_start_price

*  week_start_price = close on the first trading day of that ISO week.
*  week_end_price   = close on the last trading day of that ISO week.

A week must have at least two trading days to be considered.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def compute_weekly_decrease(validated_df: DataFrame) -> DataFrame:
    weekly = (
        validated_df.select("symbol", "date", "close")
        .withColumn(
            "week_monday",
            F.date_trunc("week", F.col("date")).cast("date"),
        )
    )

    window_first = Window.partitionBy("symbol", "week_monday").orderBy("date")
    window_last = Window.partitionBy("symbol", "week_monday").orderBy(
        F.col("date").desc()
    )

    annotated = (
        weekly.withColumn("_rn_first", F.row_number().over(window_first))
        .withColumn("_rn_last", F.row_number().over(window_last))
        .withColumn(
            "_n_days",
            F.count("date").over(Window.partitionBy("symbol", "week_monday")),
        )
    )

    week_starts = (
        annotated.filter(F.col("_rn_first") == 1)
        .select(
            "symbol",
            "week_monday",
            F.col("close").alias("start_price"),
            F.col("date").alias("week_start"),
            F.col("_n_days").alias("n_trading_days"),
        )
    )
    week_ends = (
        annotated.filter(F.col("_rn_last") == 1)
        .select(
            "symbol",
            "week_monday",
            F.col("close").alias("end_price"),
            F.col("date").alias("week_end"),
        )
    )

    weekly_returns = (
        week_starts.join(week_ends, on=["symbol", "week_monday"], how="inner")
        .filter(F.col("n_trading_days") >= 2)
        .filter(F.col("start_price") > 0)
        .withColumn("year", F.year("week_monday"))
        .withColumn("week_of_year", F.weekofyear("week_monday"))
        .withColumn(
            "weekly_return",
            (F.col("end_price") - F.col("start_price")) / F.col("start_price"),
        )
        .withColumn(
            "weekly_return_pct",
            F.round(F.col("weekly_return") * 100, 4),
        )
        .drop("n_trading_days", "week_monday")
    )

    result = (
        weekly_returns.orderBy(F.col("weekly_return").asc())
        .limit(1)
        .select(
            "symbol",
            "year",
            "week_of_year",
            "week_start",
            "week_end",
            "start_price",
            "end_price",
            "weekly_return",
            "weekly_return_pct",
        )
    )

    row = result.first()
    if row:
        logger.info(
            "Greatest weekly decrease: %s  week %d/%d  %.2f%%",
            row["symbol"],
            row["year"],
            row["week_of_year"],
            row["weekly_return_pct"],
        )

    return result