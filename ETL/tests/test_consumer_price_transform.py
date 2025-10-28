"""Tests for the Spark consumer price transformations."""

from pyspark.sql import Row
import pytest

from spark.spark.consumer_price import (
    CollectorRegistry,
    Counter,
    Gauge,
    main_schema,
    record_streaming_metrics,
    transform_coin_dataframe,
)


def test_transform_coin_dataframe_casts_numeric_fields(spark_session):
    sample_row = Row(
        data=[
            Row(
                id="bitcoin",
                symbol="BTC",
                name="Bitcoin",
                nameid="bitcoin",
                rank=1,
                price_usd="50000.50",
                percent_change_24h="5.5",
                percent_change_1h="0.5",
                percent_change_7d="10.1",
                price_btc="1",
                market_cap_usd="1000000000",
                volume24=123.4,
                volume24a=120.1,
                csupply="19000000",
                tsupply="21000000",
                msupply="21000000",
            )
        ],
        info=Row(coins_num=1, time=1_700_000_000),
    )

    input_df = spark_session.createDataFrame([sample_row], schema=main_schema)

    transformed_df = transform_coin_dataframe(input_df)

    result_row = transformed_df.select(
        "price_usd_numeric",
        "percent_change_24h_numeric",
        "percent_change_1h_numeric",
        "percent_change_7d_numeric",
        "price_btc_numeric",
        "market_cap_usd_numeric",
        "csupply_numeric",
        "tsupply_numeric",
        "msupply_numeric",
        "total_coins",
        "api_timestamp",
        "date",
    ).collect()[0]

    assert result_row.price_usd_numeric == pytest.approx(50000.50)
    assert result_row.percent_change_24h_numeric == pytest.approx(5.5)
    assert result_row.percent_change_1h_numeric == pytest.approx(0.5)
    assert result_row.percent_change_7d_numeric == pytest.approx(10.1)
    assert result_row.price_btc_numeric == pytest.approx(1.0)
    assert result_row.market_cap_usd_numeric == pytest.approx(1_000_000_000.0)
    assert result_row.csupply_numeric == pytest.approx(19_000_000.0)
    assert result_row.tsupply_numeric == pytest.approx(21_000_000.0)
    assert result_row.msupply_numeric == pytest.approx(21_000_000.0)
    assert result_row.total_coins == 1
    assert result_row.api_timestamp == 1_700_000_000
    assert result_row.date is not None


def test_transform_coin_dataframe_includes_ingestion_timestamp(spark_session):
    sample_row = Row(
        data=[
            Row(
                id="ethereum",
                symbol="ETH",
                name="Ethereum",
                nameid="ethereum",
                rank=2,
                price_usd="2000",
                percent_change_24h="1.5",
                percent_change_1h="0.2",
                percent_change_7d="3.1",
                price_btc="0.05",
                market_cap_usd="500000000",
                volume24=50.0,
                volume24a=49.5,
                csupply="120000000",
                tsupply="120000000",
                msupply="120000000",
            )
        ],
        info=Row(coins_num=1, time=1_700_000_100),
    )

    input_df = spark_session.createDataFrame([sample_row], schema=main_schema)

    transformed_df = transform_coin_dataframe(input_df)

    columns = transformed_df.columns
    assert "ingestion_timestamp" in columns
    assert "date" in columns

    collected = transformed_df.select("ingestion_timestamp").collect()
    assert collected[0]["ingestion_timestamp"] is not None


def test_record_streaming_metrics_updates_custom_registry():
    registry = CollectorRegistry()
    records_counter = Counter(
        "test_records_total", "Number of records processed during tests.", registry=registry
    )
    duration_gauge = Gauge(
        "test_trigger_duration_seconds",
        "Duration of the last trigger execution in seconds during tests.",
        registry=registry,
    )

    progress_payload = {"numInputRows": 5, "durationMs": {"triggerExecution": 2000}}

    record_streaming_metrics(
        progress_payload,
        records_metric=records_counter,
        trigger_duration_metric=duration_gauge,
    )

    records_value = (
        registry.get_sample_value("test_records_total")
        if hasattr(registry, "get_sample_value")
        else getattr(records_counter, "_value", None)
    )
    duration_value = (
        registry.get_sample_value("test_trigger_duration_seconds")
        if hasattr(registry, "get_sample_value")
        else getattr(duration_gauge, "_value", None)
    )

    assert records_value == 5
    assert duration_value == pytest.approx(2.0)
