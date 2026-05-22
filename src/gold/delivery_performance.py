import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count, countDistinct, sum, avg, round, when, col, lit
)
from config.settings import SILVER_PATH, GOLD_DELIVERY_PERFORMANCE


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SupplyChain_Gold_DeliveryPerformance")
        .master("local[*]")
        .getOrCreate()
    )


def read_silver(spark):
    return spark.read.parquet(SILVER_PATH)


def build_delivery_performance(df):
    return (
        df.groupBy("market", "order_region")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct(
                when(col("delivery_status") == "Late delivery", col("order_id"))
            ).alias("late_deliveries"),
            countDistinct(
                when(col("delivery_status") == "Shipping on time", col("order_id"))
            ).alias("on_time_deliveries"),
            countDistinct(
                when(col("delivery_status") == "Advance shipping", col("order_id"))
            ).alias("advance_deliveries"),
            countDistinct(
                when(col("delivery_status") == "Shipping canceled", col("order_id"))
            ).alias("canceled_deliveries"),
            round(avg("days_shipping_real"), 2).alias("avg_days_shipping_real"),
            round(avg("days_shipping_scheduled"), 2).alias("avg_days_shipping_scheduled"),
            round(
                avg(col("days_shipping_real") - col("days_shipping_scheduled")), 2
            ).alias("avg_shipping_delay_days"),
        )
        .withColumn(
            "late_delivery_rate_pct",
            round((col("late_deliveries") / col("total_orders")) * 100, 2)
        )
        .orderBy("market", "order_region")
    )


def write_gold(df):
    (
        df.write
        .mode("overwrite")
        .parquet(GOLD_DELIVERY_PERFORMANCE)
    )


def main():
    spark = create_spark_session()

    print("Reading silver layer...")
    df = read_silver(spark)

    print("Building delivery performance aggregation...")
    result = build_delivery_performance(df)

    print(f"Writing to {GOLD_DELIVERY_PERFORMANCE}...")
    write_gold(result)

    print("Delivery performance gold table complete.")
    result.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
