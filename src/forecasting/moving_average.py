import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, lit, to_date, date_format,
    year, month, round, lag, date_trunc
)
from pyspark.sql.window import Window
from config.settings import SILVER_PATH


TEST_START   = "2017-01-01"
WINDOW_SIZES = [4, 8, 12]  # weeks


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SupplyChain_Forecasting_MovingAverage")
        .master("local[*]")
        .getOrCreate()
    )


def read_silver(spark):
    return spark.read.parquet(SILVER_PATH)


def build_weekly_actuals(df):
    """Aggregate order_item_quantity to weekly level by market + category."""
    return (
        df
        .withColumn("week", date_trunc("week", col("order_date")))
        .groupBy("market", "category_name", "week")
        .agg(sum("order_item_quantity").alias("actual_quantity"))
        .orderBy("market", "category_name", "week")
    )


def compute_moving_average(df, window_size: int):
    """Compute moving average forecast for each market + category."""
    w = (
        Window
        .partitionBy("market", "category_name")
        .orderBy("week")
        .rowsBetween(-window_size, -1)
    )
    return (
        df
        .withColumn(f"ma_{window_size}w", round(avg("actual_quantity").over(w), 2))
    )


def split_train_test(df):
    train = df.filter(col("week") < to_date(lit(TEST_START)))
    test  = df.filter(col("week") >= to_date(lit(TEST_START)))
    return train, test


def get_forecast(spark):
    df      = read_silver(spark)
    weekly  = build_weekly_actuals(df)

    for w in WINDOW_SIZES:
        weekly = compute_moving_average(weekly, w)

    return weekly


if __name__ == "__main__":
    spark = create_spark_session()

    print("Building weekly actuals and moving average forecasts...")
    result = get_forecast(spark)

    train, test = split_train_test(result)

    print(f"Train weeks: {train.count()}")
    print(f"Test weeks:  {test.count()}")
    print("\nMarkets in test set:")
    test.groupBy("market").count().show()
    print("\nSample forecast (test set):")
    test.show(10, truncate=False)

    spark.stop()
