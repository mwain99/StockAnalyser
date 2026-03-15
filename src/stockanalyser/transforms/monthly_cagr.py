"""
monthly_cagr.py
---------------
Transform 3: Greatest monthly CAGR between January and June.

Monthly CAGR formula
~~~~~~~~~~~~~~~~~~~~
Using the same formula structure as annual CAGR but treating each
calendar month as one period:

    monthly_CAGR = (end_price / start_price) ^ (1 / n_months) - 1

where:
    start_price  = closing price on the last trading day of January
    end_price    = closing price on the last trading day of June
    n_months     = 5  (Jan → Feb → Mar → Apr → May → Jun = 5 steps)

Edge cases
~~~~~~~~~~
* Tickers missing January or June data are excluded.
* start_price of 0 is excluded (divide-by-zero guard).
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

_N_MONTHS: int = 5


def compute_monthly_cagr(
    validated_df: DataFrame,
    n_months: int = _N_MONTHS,
) -> DataFrame:

    monthly = validated_df.select(
        "symbol", "date", "close"
    ).withColumn("month", F.month("date"))

    jan = (
        monthly.filter(F.col("month") == 1)
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("symbol").orderBy(F.col("date").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(
            F.col("symbol"),
            F.col("close").alias("start_price"),
            F.col("date").alias("jan_date"),
        )
    )

    jun = (
        monthly.filter(F.col("month") == 6)
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("symbol").orderBy(F.col("date").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(
            F.col("symbol"),
            F.col("close").alias("end_price"),
            F.col("date").alias("jun_date"),
        )
    )

    result = (
        jan.join(jun, on="symbol", how="inner")
        .filter(F.col("start_price") > 0)
        .withColumn(
            "monthly_cagr",
            F.pow(
                F.col("end_price") / F.col("start_price"),
                F.lit(1.0 / n_months),
            )
            - F.lit(1.0),
        )
        .withColumn("n_months", F.lit(n_months).cast("integer"))
        .withColumn(
            "rank",
            F.rank()
            .over(Window.orderBy(F.col("monthly_cagr").desc()))
            .cast("integer"),
        )
        .drop("jan_date", "jun_date")
        .orderBy("rank")
    )

    winner = result.filter(F.col("rank") == 1).first()
    if winner:
        logger.info(
            "Greatest monthly CAGR: %s  %.4f (%.2f%% per month)",
            winner["symbol"],
            winner["monthly_cagr"],
            winner["monthly_cagr"] * 100,
        )

    return result
