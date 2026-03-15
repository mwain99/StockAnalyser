"""
stock_schema.py
---------------
PySpark StructType schemas for every data layer in the pipeline.

Design notes
~~~~~~~~~~~~
* Having schemas defined here (rather than inferred) gives us:
    - Explicit column types → avoids silent type coercions
    - Early-fail validation on ingested data
    - A single place to update when the API contract changes
* The three layers follow a classic medallion pattern:
    RAW → VALIDATED → ENRICHED
"""
from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------- #
#  Input – S&P 500 company list                                                #
# --------------------------------------------------------------------------- #

SP500_INPUT_SCHEMA = StructType(
    [
        StructField("name", StringType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
    ]
)

# --------------------------------------------------------------------------- #
#  Raw layer – as returned by the Polygon /v2/aggs endpoint                    #
# --------------------------------------------------------------------------- #

RAW_STOCK_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("date", StringType(), nullable=False),   # "YYYY-MM-DD"
        StructField("open", DoubleType(), nullable=True),
        StructField("high", DoubleType(), nullable=True),
        StructField("low", DoubleType(), nullable=True),
        StructField("close", DoubleType(), nullable=True),
        StructField("volume", LongType(), nullable=True),
        StructField("vwap", DoubleType(), nullable=True),
        StructField("transactions", LongType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)

# --------------------------------------------------------------------------- #
#  Validated layer – types enforced, nulls checked                             #
# --------------------------------------------------------------------------- #

VALIDATED_STOCK_SCHEMA = StructType(
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

# --------------------------------------------------------------------------- #
#  Transform output schemas                                                    #
# --------------------------------------------------------------------------- #

# Transform 1 – relative price increase
RELATIVE_INCREASE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("start_price", DoubleType(), nullable=False),
        StructField("end_price", DoubleType(), nullable=False),
        StructField("relative_increase", DoubleType(), nullable=False),  # fraction
        StructField("relative_increase_pct", DoubleType(), nullable=False),
        StructField("rank", IntegerType(), nullable=False),
    ]
)

# Transform 2 – portfolio value
PORTFOLIO_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("start_price", DoubleType(), nullable=False),
        StructField("end_price", DoubleType(), nullable=False),
        StructField("shares_purchased", LongType(), nullable=False),
        StructField("capital_invested", DoubleType(), nullable=False),
        StructField("uninvested_cash", DoubleType(), nullable=False),
        StructField("current_value", DoubleType(), nullable=False),
        StructField("pnl", DoubleType(), nullable=False),
    ]
)

PORTFOLIO_SUMMARY_SCHEMA = StructType(
    [
        StructField("total_capital_invested", DoubleType(), nullable=False),
        StructField("total_uninvested_cash", DoubleType(), nullable=False),
        StructField("total_current_value", DoubleType(), nullable=False),
        StructField("total_pnl", DoubleType(), nullable=False),
        StructField("total_return_pct", DoubleType(), nullable=False),
    ]
)

# Transform 3 – monthly CAGR
MONTHLY_CAGR_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("start_price", DoubleType(), nullable=False),
        StructField("end_price", DoubleType(), nullable=False),
        StructField("n_months", IntegerType(), nullable=False),
        StructField("monthly_cagr", DoubleType(), nullable=False),
        StructField("rank", IntegerType(), nullable=False),
    ]
)

# Transform 4 – greatest weekly decrease
WEEKLY_DECREASE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField("week_of_year", IntegerType(), nullable=False),
        StructField("week_start", DateType(), nullable=False),
        StructField("week_end", DateType(), nullable=False),
        StructField("start_price", DoubleType(), nullable=False),
        StructField("end_price", DoubleType(), nullable=False),
        StructField("weekly_return", DoubleType(), nullable=False),   # fraction
        StructField("weekly_return_pct", DoubleType(), nullable=False),
    ]
)
