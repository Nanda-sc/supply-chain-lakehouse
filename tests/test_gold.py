import sys
import os
import pytest
from datetime import date

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Row
from src.gold.delivery_performance import build_delivery_performance
from src.gold.supplier_performance import build_supplier_performance
from src.gold.shipping_analysis import build_shipping_analysis


def make_silver_df(spark):
    return spark.createDataFrame([
        Row(
            order_id=1, order_item_id=101, customer_id=10,
            order_date=date(2018, 1, 15),
            market="Europe", order_region="Western Europe",
            delivery_status="Late delivery", late_delivery_risk=1,
            days_shipping_real=4, days_shipping_scheduled=2,
            shipping_mode="Second Class",
            department_name="Fan Shop", category_name="Fishing",
            order_item_quantity=2, sales=200.0,
            order_profit_per_order=20.0, order_item_discount_rate=0.1,
            order_item_profit_ratio=0.1, product_price=100.0,
        ),
        Row(
            order_id=2, order_item_id=102, customer_id=11,
            order_date=date(2018, 1, 16),
            market="Europe", order_region="Western Europe",
            delivery_status="Shipping on time", late_delivery_risk=0,
            days_shipping_real=2, days_shipping_scheduled=2,
            shipping_mode="Second Class",
            department_name="Fan Shop", category_name="Fishing",
            order_item_quantity=1, sales=100.0,
            order_profit_per_order=15.0, order_item_discount_rate=0.1,
            order_item_profit_ratio=0.15, product_price=100.0,
        ),
        Row(
            order_id=3, order_item_id=103, customer_id=12,
            order_date=date(2018, 1, 17),
            market="LATAM", order_region="South America",
            delivery_status="Advance shipping", late_delivery_risk=0,
            days_shipping_real=1, days_shipping_scheduled=4,
            shipping_mode="Standard Class",
            department_name="Apparel", category_name="Cleats",
            order_item_quantity=3, sales=150.0,
            order_profit_per_order=30.0, order_item_discount_rate=0.05,
            order_item_profit_ratio=0.2, product_price=50.0,
        ),
        Row(
            order_id=4, order_item_id=104, customer_id=13,
            order_date=date(2018, 1, 18),
            market="LATAM", order_region="South America",
            delivery_status="Shipping canceled", late_delivery_risk=0,
            days_shipping_real=3, days_shipping_scheduled=4,
            shipping_mode="Standard Class",
            department_name="Apparel", category_name="Cleats",
            order_item_quantity=1, sales=50.0,
            order_profit_per_order=5.0, order_item_discount_rate=0.05,
            order_item_profit_ratio=0.1, product_price=50.0,
        ),
    ])


# --- delivery_performance ---

def test_delivery_performance_row_count(spark):
    df = make_silver_df(spark)
    result = build_delivery_performance(df)
    assert result.count() == 2


def test_delivery_performance_columns_exist(spark):
    df = make_silver_df(spark)
    result = build_delivery_performance(df)
    expected = [
        "market", "order_region", "total_orders",
        "late_deliveries", "on_time_deliveries",
        "advance_deliveries", "canceled_deliveries",
        "late_delivery_rate_pct"
    ]
    for col in expected:
        assert col in result.columns


def test_delivery_performance_late_rate(spark):
    df = make_silver_df(spark)
    result = build_delivery_performance(df)
    europe = result.filter(result.market == "Europe").collect()[0]
    assert europe["total_orders"] == 2
    assert europe["late_deliveries"] == 1
    assert europe["late_delivery_rate_pct"] == 50.0


def test_delivery_performance_on_time(spark):
    df = make_silver_df(spark)
    result = build_delivery_performance(df)
    europe = result.filter(result.market == "Europe").collect()[0]
    assert europe["on_time_deliveries"] == 1


def test_delivery_performance_groups_by_market_region(spark):
    df = make_silver_df(spark)
    result = build_delivery_performance(df)
    markets = [r["market"] for r in result.collect()]
    assert "Europe" in markets
    assert "LATAM" in markets


# --- supplier_performance ---

def test_supplier_performance_row_count(spark):
    df = make_silver_df(spark)
    result = build_supplier_performance(df)
    assert result.count() == 2


def test_supplier_performance_columns_exist(spark):
    df = make_silver_df(spark)
    result = build_supplier_performance(df)
    expected = [
        "department_name", "category_name", "total_orders",
        "total_units_sold", "total_revenue", "total_profit",
        "avg_discount_rate", "avg_profit_ratio",
        "avg_product_price", "profit_margin_pct"
    ]
    for col in expected:
        assert col in result.columns


def test_supplier_performance_revenue(spark):
    df = make_silver_df(spark)
    result = build_supplier_performance(df)
    fishing = result.filter(result.category_name == "Fishing").collect()[0]
    assert fishing["total_revenue"] == 300.0


def test_supplier_performance_profit_margin(spark):
    df = make_silver_df(spark)
    result = build_supplier_performance(df)
    fishing = result.filter(result.category_name == "Fishing").collect()[0]
    assert fishing["total_profit"] == 35.0
    assert fishing["profit_margin_pct"] == round((35.0 / 300.0) * 100, 2)


def test_supplier_performance_units_sold(spark):
    df = make_silver_df(spark)
    result = build_supplier_performance(df)
    cleats = result.filter(result.category_name == "Cleats").collect()[0]
    assert cleats["total_units_sold"] == 4


# --- shipping_analysis ---

def test_shipping_analysis_row_count(spark):
    df = make_silver_df(spark)
    result = build_shipping_analysis(df)
    assert result.count() == 2


def test_shipping_analysis_columns_exist(spark):
    df = make_silver_df(spark)
    result = build_shipping_analysis(df)
    expected = [
        "shipping_mode", "market", "total_orders",
        "avg_days_shipping_real", "avg_days_shipping_scheduled",
        "avg_delay_days", "late_orders", "on_time_orders",
        "early_orders", "on_time_rate_pct", "late_rate_pct",
        "early_rate_pct"
    ]
    for col in expected:
        assert col in result.columns


def test_shipping_analysis_late_rate(spark):
    df = make_silver_df(spark)
    result = build_shipping_analysis(df)
    second_class = result.filter(result.shipping_mode == "Second Class").collect()[0]
    assert second_class["late_orders"] == 1
    assert second_class["late_rate_pct"] == 50.0


def test_shipping_analysis_avg_delay(spark):
    df = make_silver_df(spark)
    result = build_shipping_analysis(df)
    second_class = result.filter(result.shipping_mode == "Second Class").collect()[0]
    assert second_class["avg_delay_days"] == 1.0


def test_shipping_analysis_groups_by_mode_market(spark):
    df = make_silver_df(spark)
    result = build_shipping_analysis(df)
    modes = [r["shipping_mode"] for r in result.collect()]
    assert "Second Class" in modes
    assert "Standard Class" in modes
