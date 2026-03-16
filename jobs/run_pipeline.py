"""
run_pipeline.py
---------------
Main entry-point for the full stock-analysis pipeline.

Usage
~~~~~
    # Full run - fetch from API and run transforms
    python jobs/run_pipeline.py --input data/stocks_list.csv

    # Skip ingestion - use already-saved raw Parquet and run transforms only
    python jobs/run_pipeline.py --skip-ingestion
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stockanalyser.config.settings import settings
from stockanalyser.ingestion.ingestor import StockDataIngestor
from stockanalyser.spark_session import get_spark
from stockanalyser.transforms.monthly_cagr import compute_monthly_cagr
from stockanalyser.transforms.portfolio import compute_portfolio
from stockanalyser.transforms.relative_increase import compute_relative_increase
from stockanalyser.transforms.weekly_decrease import compute_weekly_decrease
from stockanalyser.validation.validators import StockDataValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_pipeline")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stock Analyser Pipeline")
    parser.add_argument(
        "--input",
        default=str(settings.input_file),
        help="Path to the S&P 500 company list CSV (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        default=str(settings.output_dir),
        help="Root output directory (default: %(default)s)",
    )
    parser.add_argument(
        "--start-date",
        default=settings.start_date,
        help="Start date YYYY-MM-DD (default: %(default)s)",
    )
    parser.add_argument(
        "--end-date",
        default=settings.end_date,
        help="End date YYYY-MM-DD (default: %(default)s)",
    )
    parser.add_argument(
        "--validate-tickers",
        action="store_true",
        dest="validate_tickers",
        help=(
            "Run pre-flight ticker validation against the Polygon reference API "
            "before fetching bar data. Recommended when using a new or modified "
            "input file. Adds ~20 mins on the free tier."
        ),
    )
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help=(
            "Skip the API fetch and load raw data from output/raw instead. "
            "Useful for re-running transforms without hitting the API again."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = Path(args.output)
    output_root.mkdir(parents=True, exist_ok=True)
    raw_path = output_root / "raw"

    spark = get_spark()
    logger.info("SparkSession ready (%s)", spark.version)

    # ------------------------------------------------------------------ #
    #  Stage 1 – Ingestion                                                 #
    # ------------------------------------------------------------------ #
    if args.skip_ingestion:
        if not raw_path.exists():
            logger.error(
                "--skip-ingestion was set but no raw data found at '%s'. "
                "Run without --skip-ingestion first to fetch from the API.",
                raw_path,
            )
            sys.exit(1)
        logger.info("=== STAGE 1: SKIPPED (loading from %s) ===", raw_path)
        raw_df = spark.read.parquet(str(raw_path))
        logger.info("Loaded %d raw records from disk", raw_df.count())
    else:
        logger.info("=== STAGE 1: INGESTION ===")
        ingestor = StockDataIngestor(spark, run_validation=args.validate_tickers)
        raw_df = ingestor.ingest(
            input_path=args.input,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        raw_df.cache()
        logger.info("Raw records fetched: %d", raw_df.count())

        raw_df.write.mode("overwrite").partitionBy("symbol").parquet(str(raw_path))
        logger.info("Raw data saved to %s", raw_path)

    # ------------------------------------------------------------------ #
    #  Stage 2 – Validation                                                #
    # ------------------------------------------------------------------ #
    logger.info("=== STAGE 2: VALIDATION ===")
    validator = StockDataValidator()
    result = validator.validate(raw_df)

    valid_df = result.valid.cache()
    logger.info(
        "Valid: %d  Quarantine: %d  Pass rate: %.1f%%",
        result.total_valid,
        result.total_quarantine,
        result.pass_rate * 100,
    )

    if result.total_quarantine > 0:
        result.quarantine.write.mode("overwrite").parquet(str(output_root / "quarantine"))
        logger.warning("Quarantine data written to %s/quarantine", output_root)

    # ------------------------------------------------------------------ #
    #  Stage 3 – Transforms                                                #
    # ------------------------------------------------------------------ #
    logger.info("=== STAGE 3: TRANSFORMS ===")

    logger.info("-- Transform 1: Relative price increase --")
    t1 = compute_relative_increase(valid_df)
    t1.write.mode("overwrite").parquet(str(output_root / "t1_relative_increase"))
    top_stock = t1.filter(t1.rank == 1).first()
    logger.info(
        "RESULT T1 → Greatest relative increase: %s  (+%.2f%%)",
        top_stock["symbol"],
        top_stock["relative_increase_pct"],
    )

    logger.info("-- Transform 2: Portfolio simulation --")
    t2_detail, t2_summary = compute_portfolio(
        valid_df, investment_per_stock=settings.investment_per_stock
    )
    t2_detail.write.mode("overwrite").parquet(str(output_root / "t2_portfolio_detail"))
    logger.info(
        "RESULT T2 → Portfolio value: $%.2f  PnL: $%.2f (%.2f%%)",
        t2_summary.total_current_value,
        t2_summary.total_pnl,
        t2_summary.total_return_pct,
    )

    logger.info("-- Transform 3: Monthly CAGR (Jan-June) --")
    t3 = compute_monthly_cagr(valid_df)
    t3.write.mode("overwrite").parquet(str(output_root / "t3_monthly_cagr"))
    top_cagr = t3.filter(t3.rank == 1).first()
    logger.info(
        "RESULT T3 → Greatest monthly CAGR: %s  (%.4f per month = %.2f%%/month)",
        top_cagr["symbol"],
        top_cagr["monthly_cagr"],
        top_cagr["monthly_cagr"] * 100,
    )

    logger.info("-- Transform 4: Greatest weekly decrease --")
    t4 = compute_weekly_decrease(valid_df)
    t4.write.mode("overwrite").parquet(str(output_root / "t4_weekly_decrease"))
    worst_week = t4.first()
    logger.info(
        "RESULT T4 → Greatest weekly decrease: %s  week %d/%d  (%.2f%%)",
        worst_week["symbol"],
        worst_week["year"],
        worst_week["week_of_year"],
        worst_week["weekly_return_pct"],
    )

    # ------------------------------------------------------------------ #
    #  Summary                                                             #
    # ------------------------------------------------------------------ #
    logger.info("=== PIPELINE COMPLETE ===")
    logger.info("Output directory: %s", output_root.resolve())

    spark.stop()


if __name__ == "__main__":
    main()