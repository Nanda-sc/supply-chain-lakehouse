import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, date_trunc
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType,
)
from config.settings import SILVER_PATH

TEST_START = "2017-01-01"
MIN_TRAIN_WEEKS = 8

PROPHET_SCHEMA = StructType([
    StructField("market",           StringType(),    True),
    StructField("category_name",    StringType(),    True),
    StructField("week",             TimestampType(), True),
    StructField("actual_quantity",  DoubleType(),    True),
    StructField("prophet_forecast", DoubleType(),    True),
    StructField("split",            StringType(),    True),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SupplyChain_Forecasting_Prophet")
        .master("local[*]")
        .getOrCreate()
    )


def read_silver(spark):
    return spark.read.parquet(SILVER_PATH)


def build_weekly_actuals(df):
    return (
        df
        .withColumn("week", date_trunc("week", col("order_date")))
        .groupBy("market", "category_name", "week")
        .agg(spark_sum("order_item_quantity").alias("actual_quantity"))
        .orderBy("market", "category_name", "week")
    )


def _fit_prophet_group(pdf: pd.DataFrame) -> pd.DataFrame:
    logging.getLogger("prophet").setLevel(logging.ERROR)
    logging.getLogger("cmdstanpy").setLevel(logging.ERROR)

    from prophet import Prophet

    pdf = pdf.sort_values("week").reset_index(drop=True)
    pdf["week"] = pd.to_datetime(pdf["week"])

    train_mask = pdf["week"] < pd.Timestamp(TEST_START)
    train = pdf[train_mask].copy()

    result = pdf.copy()
    result["split"] = "train"
    result.loc[~train_mask, "split"] = "test"
    result["actual_quantity"] = result["actual_quantity"].astype(float)

    if len(train) < MIN_TRAIN_WEEKS:
        result["prophet_forecast"] = None
        return result[["market", "category_name", "week", "actual_quantity", "prophet_forecast", "split"]]

    prophet_df = train[["week", "actual_quantity"]].rename(
        columns={"week": "ds", "actual_quantity": "y"}
    )

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        seasonality_mode="multiplicative",
    )
    model.fit(prophet_df)

    future   = pd.DataFrame({"ds": pdf["week"]})
    forecast = model.predict(future)

    result["prophet_forecast"] = forecast["yhat"].clip(lower=0).round(2).values

    return result[["market", "category_name", "week", "actual_quantity", "prophet_forecast", "split"]]


def get_prophet_forecast(spark):
    df     = read_silver(spark)
    weekly = build_weekly_actuals(df)
    return weekly.groupBy("market", "category_name").applyInPandas(
        _fit_prophet_group, schema=PROPHET_SCHEMA
    )


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("Training Prophet models per market + category...")
    result = get_prophet_forecast(spark)

    test = result.filter(col("split") == "test")
    print(f"\nTest rows: {test.count()}")
    print("\nMarkets in test set:")
    test.groupBy("market").count().show()
    print("\nSample Prophet forecast (test set):")
    test.show(10, truncate=False)

    spark.stop()
