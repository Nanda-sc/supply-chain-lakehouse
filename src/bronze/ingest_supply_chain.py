import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, col
from config.settings import RAW_DATA_PATH, BRONZE_PATH, SOURCE_FILE_NAME


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SupplyChain_Bronze_Ingestion")
        .master("local[*]")
        .getOrCreate()
    )


def read_raw_csv(spark):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",")
        .csv(RAW_DATA_PATH)
    )


def add_metadata(df):
    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_name", lit(SOURCE_FILE_NAME))
    )


def parse_order_date(df):
    return (
        df
        .withColumn("order_date", to_date(col("order date (DateOrders)"), "M/d/yyyy H:mm"))
    )


def write_bronze(df):
    (
        df.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(BRONZE_PATH)
    )


def main():
    spark = create_spark_session()

    print("Reading raw CSV...")
    df = read_raw_csv(spark)

    print("Adding metadata columns...")
    df = add_metadata(df)

    print("Parsing order date...")
    df = parse_order_date(df)

    print(f"Writing to bronze layer at {BRONZE_PATH}...")
    write_bronze(df)

    print("Bronze ingestion complete.")
    print(f"Total records: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
