import sys
import os
import pytest
from datetime import date

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.bronze.ingest_supply_chain import add_metadata, parse_order_date


def make_raw_df(spark):
    return spark.createDataFrame(
        [
            Row(**{"Order Id": 1, "order date (DateOrders)": "1/15/2018 10:30", "Order Status": "COMPLETE"}),
            Row(**{"Order Id": 2, "order date (DateOrders)": "3/5/2017 8:00",  "Order Status": "PENDING"}),
            Row(**{"Order Id": 3, "order date (DateOrders)": "12/1/2016 22:45", "Order Status": "COMPLETE"}),
        ]
    )


def test_add_metadata_adds_ingestion_timestamp(spark):
    df = make_raw_df(spark)
    result = add_metadata(df)
    assert "ingestion_timestamp" in result.columns


def test_add_metadata_adds_source_file_name(spark):
    df = make_raw_df(spark)
    result = add_metadata(df)
    assert "source_file_name" in result.columns
    values = [r["source_file_name"] for r in result.collect()]
    assert all(v == "DataCoSupplyChainDataset.csv" for v in values)


def test_add_metadata_preserves_row_count(spark):
    df = make_raw_df(spark)
    result = add_metadata(df)
    assert result.count() == df.count()


def test_parse_order_date_creates_date_column(spark):
    df = make_raw_df(spark)
    result = parse_order_date(df)
    assert "order_date" in result.columns


def test_parse_order_date_correct_values(spark):
    df = make_raw_df(spark)
    result = parse_order_date(df)
    dates = {r["Order Id"]: r["order_date"] for r in result.collect()}
    assert dates[1] == date(2018, 1, 15)
    assert dates[2] == date(2017, 3, 5)
    assert dates[3] == date(2016, 12, 1)


def test_parse_order_date_preserves_row_count(spark):
    df = make_raw_df(spark)
    result = parse_order_date(df)
    assert result.count() == df.count()
