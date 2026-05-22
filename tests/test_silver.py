import sys
import os
import pytest
from datetime import date

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Row
from src.silver.transform_supply_chain import (
    drop_pii_columns,
    rename_columns,
    parse_shipping_date,
    apply_quality_checks,
    add_silver_metadata,
)


def make_bronze_df(spark):
    return spark.createDataFrame([
        Row(**{
            "Order Id": 1,
            "Order Item Id": 101,
            "Customer Id": 10,
            "order date (DateOrders)": "1/15/2018 10:30",
            "shipping date (DateOrders)": "1/20/2018 9:00",
            "order_date": date(2018, 1, 15),
            "Customer Email": "alice@example.com",
            "Customer Password": "secret123",
            "Customer Street": "123 Main St",
            "Product Description": "A long product description",
            "Product Image": "http://example.com/img.jpg",
            "Delivery Status": "Advance shipping",
            "Shipping Mode": "Standard Class",
            "ingestion_timestamp": "2026-05-23 00:00:00",
            "source_file_name": "DataCoSupplyChainDataset.csv",
        }),
        Row(**{
            "Order Id": 2,
            "Order Item Id": 102,
            "Customer Id": 11,
            "order date (DateOrders)": "3/5/2017 8:00",
            "shipping date (DateOrders)": "3/10/2017 14:00",
            "order_date": date(2017, 3, 5),
            "Customer Email": "bob@example.com",
            "Customer Password": "pass456",
            "Customer Street": "456 Oak Ave",
            "Product Description": "Another description",
            "Product Image": "http://example.com/img2.jpg",
            "Delivery Status": "Late delivery",
            "Shipping Mode": "First Class",
            "ingestion_timestamp": "2026-05-23 00:00:00",
            "source_file_name": "DataCoSupplyChainDataset.csv",
        }),
    ])


# --- drop_pii_columns ---

def test_drop_pii_removes_email(spark):
    df = make_bronze_df(spark)
    result = drop_pii_columns(df)
    assert "Customer Email" not in result.columns


def test_drop_pii_removes_password(spark):
    df = make_bronze_df(spark)
    result = drop_pii_columns(df)
    assert "Customer Password" not in result.columns


def test_drop_pii_removes_street(spark):
    df = make_bronze_df(spark)
    result = drop_pii_columns(df)
    assert "Customer Street" not in result.columns


def test_drop_pii_removes_junk_columns(spark):
    df = make_bronze_df(spark)
    result = drop_pii_columns(df)
    assert "Product Description" not in result.columns
    assert "Product Image" not in result.columns


def test_drop_pii_preserves_row_count(spark):
    df = make_bronze_df(spark)
    result = drop_pii_columns(df)
    assert result.count() == df.count()


# --- rename_columns ---

def test_rename_columns_delivery_status(spark):
    df = make_bronze_df(spark)
    result = rename_columns(df)
    assert "delivery_status" in result.columns
    assert "Delivery Status" not in result.columns


def test_rename_columns_shipping_mode(spark):
    df = make_bronze_df(spark)
    result = rename_columns(df)
    assert "shipping_mode" in result.columns
    assert "Shipping Mode" not in result.columns


def test_rename_columns_order_date_raw(spark):
    df = make_bronze_df(spark)
    result = rename_columns(df)
    assert "order_date_raw" in result.columns


def test_rename_columns_preserves_row_count(spark):
    df = make_bronze_df(spark)
    result = rename_columns(df)
    assert result.count() == df.count()


# --- parse_shipping_date ---

def test_parse_shipping_date_creates_column(spark):
    df = make_bronze_df(spark)
    df = rename_columns(df)
    result = parse_shipping_date(df)
    assert "shipping_date" in result.columns


def test_parse_shipping_date_correct_values(spark):
    df = make_bronze_df(spark)
    df = rename_columns(df)
    result = parse_shipping_date(df)
    dates = {r["order_id"]: r["shipping_date"] for r in result.collect()}
    assert dates[1] == date(2018, 1, 20)
    assert dates[2] == date(2017, 3, 10)


# --- apply_quality_checks ---

def test_quality_checks_drops_duplicates(spark):
    df = spark.createDataFrame([
        Row(**{"order_id": 1, "order_item_id": 101, "customer_id": 10, "order_date": date(2018, 1, 15)}),
        Row(**{"order_id": 1, "order_item_id": 101, "customer_id": 10, "order_date": date(2018, 1, 15)}),
        Row(**{"order_id": 2, "order_item_id": 102, "customer_id": 11, "order_date": date(2017, 3, 5)}),
    ])
    result = apply_quality_checks(df)
    assert result.count() == 2


def test_quality_checks_drops_null_order_id(spark):
    df = spark.createDataFrame([
        Row(**{"order_id": None, "order_item_id": 101, "customer_id": 10, "order_date": date(2018, 1, 15)}),
        Row(**{"order_id": 2,    "order_item_id": 102, "customer_id": 11, "order_date": date(2017, 3, 5)}),
    ])
    result = apply_quality_checks(df)
    assert result.count() == 1


def test_quality_checks_drops_null_customer_id(spark):
    df = spark.createDataFrame([
        Row(**{"order_id": 1, "order_item_id": 101, "customer_id": None, "order_date": date(2018, 1, 15)}),
        Row(**{"order_id": 2, "order_item_id": 102, "customer_id": 11,   "order_date": date(2017, 3, 5)}),
    ])
    result = apply_quality_checks(df)
    assert result.count() == 1


def test_quality_checks_drops_null_order_date(spark):
    df = spark.createDataFrame([
        Row(**{"order_id": 1, "order_item_id": 101, "customer_id": 10, "order_date": None}),
        Row(**{"order_id": 2, "order_item_id": 102, "customer_id": 11, "order_date": date(2017, 3, 5)}),
    ])
    result = apply_quality_checks(df)
    assert result.count() == 1


# --- add_silver_metadata ---

def test_add_silver_metadata_adds_timestamp(spark):
    df = make_bronze_df(spark)
    result = add_silver_metadata(df)
    assert "silver_timestamp" in result.columns


def test_add_silver_metadata_preserves_row_count(spark):
    df = make_bronze_df(spark)
    result = add_silver_metadata(df)
    assert result.count() == df.count()
