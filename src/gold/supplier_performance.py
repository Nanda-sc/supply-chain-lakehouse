import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct, sum, avg, round, col
)
from config.settings import SILVER_PATH, GOLD_SUPPLIER_PERFORMANCE


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SupplyChain_Gold_SupplierPerformance")
        .master("local[*]")
        .getOrCreate()
    )


def read_silver(spark):
    return spark.read.parquet(SILVER_PATH)


def build_supplier_performance(df):
    return (
        df.groupBy("department_name", "category_name")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("order_item_quantity").alias("total_units_sold"),
            round(sum("sales"), 2).alias("total_revenue"),
            round(sum("order_profit_per_order"), 2).alias("total_profit"),
            round(avg("order_item_discount_rate"), 4).alias("avg_discount_rate"),
            round(avg("order_item_profit_ratio"), 4).alias("avg_profit_ratio"),
            round(avg("product_price"), 2).alias("avg_product_price"),
        )
        .withColumn(
            "profit_margin_pct",
            round((col("total_profit") / col("total_revenue")) * 100, 2)
        )
        .orderBy("department_name", "category_name")
    )


def write_gold(df):
    (
        df.write
        .mode("overwrite")
        .parquet(GOLD_SUPPLIER_PERFORMANCE)
    )


def main():
    spark = create_spark_session()

    print("Reading silver layer...")
    df = read_silver(spark)

    print("Building supplier performance aggregation...")
    result = build_supplier_performance(df)

    print(f"Writing to {GOLD_SUPPLIER_PERFORMANCE}...")
    write_gold(result)

    print("Supplier performance gold table complete.")
    result.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
