import sys
import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Row
from pyspark.sql.functions import col
from src.forecasting.moving_average import (
    build_weekly_actuals,
    compute_moving_average,
    split_train_test,
    TEST_START,
)
from src.forecasting.accuracy_metrics import compute_kpis, build_accuracy_summary


# ── Fixtures ─────────────────────────────────────────────────────────────────

def make_silver_df(spark):
    rows = []
    base = datetime(2016, 1, 4)  # first Monday of 2016
    for week_offset in range(12):
        order_date = base + timedelta(weeks=week_offset)
        rows.append(Row(
            order_id=week_offset,
            order_date=order_date,
            market="Europe",
            category_name="Fishing",
            order_item_quantity=10 + week_offset,
        ))
        rows.append(Row(
            order_id=100 + week_offset,
            order_date=order_date,
            market="LATAM",
            category_name="Cleats",
            order_item_quantity=5 + week_offset,
        ))
    return spark.createDataFrame(rows)


def make_weekly_df(spark):
    """Pre-aggregated weekly DF (avoids re-running groupBy in every test)."""
    silver = make_silver_df(spark)
    return build_weekly_actuals(silver)


# ── build_weekly_actuals ──────────────────────────────────────────────────────

def test_weekly_actuals_row_count(spark):
    weekly = make_weekly_df(spark)
    # 12 weeks × 2 groups = 24 rows
    assert weekly.count() == 24


def test_weekly_actuals_columns(spark):
    weekly = make_weekly_df(spark)
    assert set(weekly.columns) == {"market", "category_name", "week", "actual_quantity"}


def test_weekly_actuals_aggregation(spark):
    silver = make_silver_df(spark)
    weekly = build_weekly_actuals(silver)
    europe = weekly.filter(col("market") == "Europe").orderBy("week").collect()
    # First week: offset=0 → quantity=10
    assert europe[0]["actual_quantity"] == 10


# ── compute_moving_average ────────────────────────────────────────────────────

def test_moving_average_column_added(spark):
    weekly = make_weekly_df(spark)
    result = compute_moving_average(weekly, 4)
    assert "ma_4w" in result.columns


def test_moving_average_value(spark):
    weekly = make_weekly_df(spark)
    result = compute_moving_average(weekly, 4)
    europe = result.filter(col("market") == "Europe").orderBy("week").collect()
    # Row index 4 is the 5th week (offset=4, quantity=14)
    # ma_4w for row 4 = avg(rows 0..3) = (10+11+12+13)/4 = 11.5
    assert europe[4]["ma_4w"] == 11.5


def test_moving_average_first_row_null(spark):
    weekly = make_weekly_df(spark)
    result = compute_moving_average(weekly, 4)
    europe = result.filter(col("market") == "Europe").orderBy("week").collect()
    # Only the very first row has no prior history → NULL
    assert europe[0]["ma_4w"] is None
    # Rows 1-3 have partial windows (1–3 prior rows available) → not NULL
    for i in range(1, 4):
        assert europe[i]["ma_4w"] is not None


def test_moving_average_all_window_sizes(spark):
    weekly = make_weekly_df(spark)
    for w in [4, 8, 12]:
        result = compute_moving_average(weekly, w)
        assert f"ma_{w}w" in result.columns


# ── split_train_test ──────────────────────────────────────────────────────────

def test_split_no_overlap(spark):
    weekly = make_weekly_df(spark)
    for w in [4, 8, 12]:
        weekly = compute_moving_average(weekly, w)
    train, test = split_train_test(weekly)
    # All weeks in fixture are 2016 — should all be in train
    assert test.count() == 0
    assert train.count() == 24


def test_split_correct_boundary(spark):
    """Rows on/after TEST_START go to test."""
    rows = [
        Row(market="Europe", category_name="Fishing",
            week=datetime(2016, 12, 26), actual_quantity=10.0,
            ma_4w=9.0, ma_8w=9.0, ma_12w=9.0),
        Row(market="Europe", category_name="Fishing",
            week=datetime(2017, 1, 2),  actual_quantity=12.0,
            ma_4w=10.0, ma_8w=10.0, ma_12w=10.0),
    ]
    df = spark.createDataFrame(rows)
    train, test = split_train_test(df)
    assert train.count() == 1
    assert test.count() == 1


# ── compute_kpis ─────────────────────────────────────────────────────────────

def test_compute_kpis_perfect_forecast():
    actual   = pd.Series([10.0, 20.0, 30.0])
    forecast = pd.Series([10.0, 20.0, 30.0])
    kpis = compute_kpis(actual, forecast)
    assert kpis["rmse"]  == 0.0
    assert kpis["mae"]   == 0.0
    assert kpis["wmape"] == 0.0
    assert kpis["bias"]  == 0.0


def test_compute_kpis_known_values():
    actual   = pd.Series([100.0, 100.0, 100.0])
    forecast = pd.Series([110.0, 90.0, 100.0])
    kpis = compute_kpis(actual, forecast)
    # errors: +10, -10, 0 → MAE=6.667, RMSE=8.165, WMAPE=6.667, Bias=0
    assert kpis["mae"]  == round(20.0 / 3, 4)
    assert kpis["bias"] == 0.0


def test_compute_kpis_positive_bias():
    actual   = pd.Series([100.0, 100.0])
    forecast = pd.Series([120.0, 120.0])  # over-forecast
    kpis = compute_kpis(actual, forecast)
    assert kpis["bias"] > 0


def test_compute_kpis_with_nulls():
    actual   = pd.Series([10.0, 20.0, 30.0])
    forecast = pd.Series([None, 20.0, 30.0])
    kpis = compute_kpis(actual, forecast)
    assert kpis["n_weeks"] == 2


def test_compute_kpis_all_nulls():
    actual   = pd.Series([10.0, 20.0])
    forecast = pd.Series([None, None])
    kpis = compute_kpis(actual, forecast)
    assert kpis["n_weeks"] == 0
    assert kpis["rmse"] is None


# ── build_accuracy_summary ───────────────────────────────────────────────────

def test_accuracy_summary_shape():
    df = pd.DataFrame({
        "market":           ["Europe"] * 5,
        "category_name":    ["Fishing"] * 5,
        "actual_quantity":  [10.0, 20.0, 30.0, 15.0, 25.0],
        "ma_4w":            [9.0,  19.0, 29.0, 14.0, 24.0],
        "ma_8w":            [8.0,  18.0, 28.0, 13.0, 23.0],
        "ma_12w":           [7.0,  17.0, 27.0, 12.0, 22.0],
        "prophet_forecast": [10.0, 20.0, 30.0, 15.0, 25.0],
    })
    result = build_accuracy_summary(df)
    # 1 group × 4 models = 4 rows
    assert len(result) == 4
    assert set(result["model"]) == {"MA_4w", "MA_8w", "MA_12w", "Prophet"}


def test_accuracy_summary_columns():
    df = pd.DataFrame({
        "market":           ["Europe"],
        "category_name":    ["Fishing"],
        "actual_quantity":  [10.0],
        "ma_4w":            [9.0],
        "ma_8w":            [8.0],
        "ma_12w":           [7.0],
        "prophet_forecast": [10.0],
    })
    result = build_accuracy_summary(df)
    for col in ["market", "category_name", "model", "rmse", "mae", "wmape", "bias", "n_weeks"]:
        assert col in result.columns
