"""
spark_session.py
----------------
Centralised Spark session factory.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from stockanalyser.config.settings import Settings, settings as default_settings


def _configure_windows() -> None:
    if sys.platform != "win32":
        return

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    here = Path(__file__).resolve()
    for parent in here.parents:
        winutils = parent / "hadoop" / "bin" / "winutils.exe"
        if winutils.exists():
            hadoop_home = str(parent / "hadoop")
            hadoop_bin = str(parent / "hadoop" / "bin")
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["hadoop.home.dir"] = hadoop_home
            current_path = os.environ.get("PATH", "")
            if hadoop_bin not in current_path:
                os.environ["PATH"] = hadoop_bin + os.pathsep + current_path
            return

    raise EnvironmentError(
        "winutils.exe not found. Run `python setup_windows.py` from the "
        "project root to download it, then try again."
    )


def get_spark(cfg: Settings = default_settings) -> SparkSession:
    """Return (or create) a configured SparkSession."""
    _configure_windows()

    builder = (
        SparkSession.builder.appName(cfg.spark_app_name)
        .master(cfg.spark_master)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
