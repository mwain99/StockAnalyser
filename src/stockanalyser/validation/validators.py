"""
validators.py
-------------
Data-quality gate between the raw ingestion layer and the transform layer.

Design notes
~~~~~~~~~~~~
* Validation produces two DataFrames: ``valid`` (passes all checks) and
  ``quarantine`` (fails at least one check).  The pipeline continues with
  valid records and writes quarantine records to a separate sink so issues
  can be investigated without blocking the run.
* Each check is expressed as a Spark Column expression so they compose
  cleanly and the filter is pushed down into the Spark plan.
* ``ValidationResult`` is a lightweight dataclass so callers can
  inspect the outcome without parsing log strings.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from stockanalyser.schemas.stock_schema import VALIDATED_STOCK_SCHEMA

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    valid: DataFrame
    quarantine: DataFrame
    total_input: int = 0
    total_valid: int = 0
    total_quarantine: int = 0
    quarantine_reasons: dict[str, int] = field(default_factory=dict)

    @property
    def pass_rate(self) -> float:
        if self.total_input == 0:
            return 1.0
        return self.total_valid / self.total_input


class StockDataValidator:
    MIN_CLOSE: float = 0.01

    def validate(self, raw_df: DataFrame) -> ValidationResult:
        total_input = raw_df.cache().count()

        typed_df = raw_df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))
        quality_flags = self._build_quality_flags(typed_df)

        valid_df = (
            quality_flags.filter(F.col("_is_valid"))
            .drop("_is_valid", "_failure_reason")
            .select([F.col(f.name) for f in VALIDATED_STOCK_SCHEMA.fields])
        )

        quarantine_df = (
            quality_flags.filter(~F.col("_is_valid"))
            .drop("_is_valid")
        )

        total_valid = valid_df.count()
        total_quarantine = quarantine_df.count()

        reasons: dict[str, int] = {}
        if total_quarantine > 0:
            reason_rows = (
                quarantine_df.groupBy("_failure_reason")
                .count()
                .collect()
            )
            reasons = {r["_failure_reason"]: r["count"] for r in reason_rows}

        logger.info(
            "Validation complete: %d/%d valid (%.1f%%)",
            total_valid,
            total_input,
            (total_valid / total_input * 100) if total_input else 0,
        )
        if reasons:
            logger.warning("Quarantine reasons: %s", reasons)

        return ValidationResult(
            valid=valid_df,
            quarantine=quarantine_df,
            total_input=total_input,
            total_valid=total_valid,
            total_quarantine=total_quarantine,
            quarantine_reasons=reasons,
        )

    # ------------------------------------------------------------------ #
    #  Rule definitions                                                    #
    # ------------------------------------------------------------------ #

    def _build_quality_flags(self, df: DataFrame) -> DataFrame:
        rules = [
            ("non_null_symbol", F.col("symbol").isNotNull() & (F.trim(F.col("symbol")) != "")),
            ("non_null_date", F.col("date").isNotNull()),
            ("non_null_close", F.col("close").isNotNull()),
            ("positive_close", F.col("close") > self.MIN_CLOSE),
            ("non_null_open", F.col("open").isNotNull()),
            ("positive_open", F.col("open") > self.MIN_CLOSE),
            ("high_gte_low", F.col("high") >= F.col("low")),
            ("high_gte_close", F.col("high") >= F.col("close")),
            ("low_lte_close", F.col("low") <= F.col("close")),
            ("non_negative_volume", F.col("volume").isNull() | (F.col("volume") >= 0)),
            ("date_is_valid_type", F.col("date").cast(DateType()).isNotNull()),
        ]

        when_expr = None
        is_valid_expr = F.lit(True)

        for rule_name, rule_expr in rules:
            is_valid_expr = is_valid_expr & rule_expr
            if when_expr is None:
                when_expr = F.when(~rule_expr, F.lit(rule_name))
            else:
                when_expr = when_expr.when(~rule_expr, F.lit(rule_name))

        if when_expr is not None:
            when_expr = when_expr.otherwise(F.lit(None))

        return df.withColumn("_is_valid", is_valid_expr).withColumn(
            "_failure_reason", when_expr
        )
