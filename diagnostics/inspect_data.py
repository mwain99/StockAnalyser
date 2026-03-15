"""
inspect_data.py
---------------
Quick diagnostic to inspect raw data for a specific symbol and date range.

Usage:
    python inspect_data.py --symbol NOW --from 2025-12-01 --to 2025-12-31
    python inspect_data.py --symbol NFLX --from 2025-01-01 --to 2025-01-14
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from stockanalyser.spark_session import get_spark
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--from", dest="date_from", default="2025-01-01")
    parser.add_argument("--to", dest="date_to", default="2025-12-31")
    parser.add_argument("--output", default="output")
    args = parser.parse_args()

    spark = get_spark()

    raw = spark.read.parquet(str(Path(args.output) / "raw"))

    print(f"\n=== Raw data for {args.symbol} ({args.date_from} → {args.date_to}) ===")
    (
        raw.filter(F.col("symbol") == args.symbol)
        .filter(F.col("date") >= args.date_from)
        .filter(F.col("date") <= args.date_to)
        .orderBy("date")
        .select("symbol", "date", "open", "high", "low", "close", "volume")
        .show(100, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()