import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct, avg, round, col, when
)
from config.settings import SILVER_PATH, GOLD_SHIPPING_ANALYSIS


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SupplyChain_Gold_ShippingAnalysis")
        .master("local[*]")
        .getOrCreate()
    )


def read_silver(spark):
    return spark.read.parquet(SILVER_PATH)


def build_shipping_analysis(df):
    return (
        df.groupBy("shipping_mode", "market")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            round(avg("days_shipping_real"), 2).alias("avg_days_shipping_real"),
            round(avg("days_shipping_scheduled"), 2).alias("avg_days_shipping_scheduled"),
            round(
                avg(col("days_shipping_real") - col("days_shipping_scheduled")), 2
            ).alias("avg_delay_days"),
            countDistinct(
                when(col("delivery_status") == "Late delivery", col("order_id"))
            ).alias("late_orders"),
            countDistinct(
                when(col("delivery_status") == "Shipping on time", col("order_id"))
            ).alias("on_time_orders"),
            countDistinct(
                when(col("delivery_status") == "Advance shipping", col("order_id"))
            ).alias("early_orders"),
        )
        .withColumn(
            "on_time_rate_pct",
            round((col("on_time_orders") / col("total_orders")) * 100, 2)
        )
        .withColumn(
            "late_rate_pct",
            round((col("late_orders") / col("total_orders")) * 100, 2)
        )
        .withColumn(
            "early_rate_pct",
            round((col("early_orders") / col("total_orders")) * 100, 2)
        )
        .orderBy("shipping_mode", "market")
    )


def write_gold(df):
    (
        df.write
        .mode("overwrite")
        .parquet(GOLD_SHIPPING_ANALYSIS)
    )


def main():
    spark = create_spark_session()

    print("Reading silver layer...")
    df = read_silver(spark)

    print("Building shipping analysis aggregation...")
    result = build_shipping_analysis(df)

    print(f"Writing to {GOLD_SHIPPING_ANALYSIS}...")
    write_gold(result)

    print("Shipping analysis gold table complete.")
    result.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
