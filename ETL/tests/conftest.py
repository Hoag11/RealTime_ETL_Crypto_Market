"""Shared pytest fixtures for the ETL project."""

import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Provide a local Spark session for unit tests."""

    # Use a deterministic warehouse location to avoid permission issues.
    warehouse_dir = os.path.join(os.getcwd(), "spark-warehouse")
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("pytest-pyspark")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()
