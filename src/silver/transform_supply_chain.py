import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, current_timestamp
from config.settings import BRONZE_PATH, SILVER_PATH


COLUMN_RENAMES = {
    "Type":                        "order_type",
    "Days for shipping (real)":    "days_shipping_real",
    "Days for shipment (scheduled)": "days_shipping_scheduled",
    "Benefit per order":           "benefit_per_order",
    "Sales per customer":          "sales_per_customer",
    "Delivery Status":             "delivery_status",
    "Late_delivery_risk":          "late_delivery_risk",
    "Category Id":                 "category_id",
    "Category Name":               "category_name",
    "Customer City":               "customer_city",
    "Customer Country":            "customer_country",
    "Customer Fname":              "customer_first_name",
    "Customer Id":                 "customer_id",
    "Customer Lname":              "customer_last_name",
    "Customer Segment":            "customer_segment",
    "Customer State":              "customer_state",
    "Customer Zipcode":            "customer_zipcode",
    "Department Id":               "department_id",
    "Department Name":             "department_name",
    "Latitude":                    "latitude",
    "Longitude":                   "longitude",
    "Market":                      "market",
    "Order City":                  "order_city",
    "Order Country":               "order_country",
    "Order Customer Id":           "order_customer_id",
    "order date (DateOrders)":     "order_date_raw",
    "Order Id":                    "order_id",
    "Order Item Cardprod Id":      "order_item_cardprod_id",
    "Order Item Discount":         "order_item_discount",
    "Order Item Discount Rate":    "order_item_discount_rate",
    "Order Item Id":               "order_item_id",
    "Order Item Product Price":    "order_item_product_price",
    "Order Item Profit Ratio":     "order_item_profit_ratio",
    "Order Item Quantity":         "order_item_quantity",
    "Sales":                       "sales",
    "Order Item Total":            "order_item_total",
    "Order Profit Per Order":      "order_profit_per_order",
    "Order Region":                "order_region",
    "Order State":                 "order_state",
    "Order Status":                "order_status",
    "Order Zipcode":               "order_zipcode",
    "Product Card Id":             "product_card_id",
    "Product Category Id":         "product_category_id",
    "Product Name":                "product_name",
    "Product Price":               "product_price",
    "Product Status":              "product_status",
    "shipping date (DateOrders)":  "shipping_date_raw",
    "Shipping Mode":               "shipping_mode",
}

PII_AND_JUNK_COLUMNS = [
    "Customer Email",
    "Customer Password",
    "Customer Street",
    "Product Description",
    "Product Image",
]

CRITICAL_COLUMNS = ["order_id", "customer_id", "order_date"]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SupplyChain_Silver_Transform")
        .master("local[*]")
        .getOrCreate()
    )


def read_bronze(spark):
    return spark.read.parquet(BRONZE_PATH)


def drop_pii_columns(df):
    existing = [c for c in PII_AND_JUNK_COLUMNS if c in df.columns]
    return df.drop(*existing)


def rename_columns(df):
    for old_name, new_name in COLUMN_RENAMES.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def parse_shipping_date(df):
    return df.withColumn(
        "shipping_date",
        to_date(col("shipping_date_raw"), "M/d/yyyy H:mm")
    )


def apply_quality_checks(df):
    before = df.count()
    df = df.dropDuplicates(["order_id", "order_item_id"])
    df = df.dropna(subset=CRITICAL_COLUMNS)
    after = df.count()
    dropped = before - after
    if dropped > 0:
        print(f"Quality check: dropped {dropped} records ({before} → {after})")
    else:
        print(f"Quality check: all {after} records passed")
    return df


def add_silver_metadata(df):
    return df.withColumn("silver_timestamp", current_timestamp())


def write_silver(df):
    (
        df.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(SILVER_PATH)
    )


def main():
    spark = create_spark_session()

    print("Reading bronze layer...")
    df = read_bronze(spark)

    print("Dropping PII and junk columns...")
    df = drop_pii_columns(df)

    print("Renaming columns to snake_case...")
    df = rename_columns(df)

    print("Parsing shipping date...")
    df = parse_shipping_date(df)

    print("Applying quality checks...")
    df = apply_quality_checks(df)

    print("Adding silver metadata...")
    df = add_silver_metadata(df)

    print(f"Writing to silver layer at {SILVER_PATH}...")
    write_silver(df)

    print("Silver transformation complete.")
    print(f"Total records: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
