import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, when, isnan, nanvl

from config.settings import GOLD_FORECAST_RESULTS, GOLD_FORECAST_ACCURACY
from src.forecasting.moving_average import get_forecast as get_ma_forecast, TEST_START
from src.forecasting.prophet_forecast import get_prophet_forecast
from src.forecasting.accuracy_metrics import build_accuracy_summary


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SupplyChain_Gold_ForecastAccuracy")
        .master("local[*]")
        .getOrCreate()
    )


def build_forecast_results(spark):
    """Join MA + Prophet forecasts into a single weekly table (all splits)."""
    ma_df      = get_ma_forecast(spark)
    prophet_df = get_prophet_forecast(spark)

    combined = ma_df.join(
        prophet_df.select("market", "category_name", "week", "prophet_forecast", "split"),
        on=["market", "category_name", "week"],
        how="left",
    )

    # Backfill split label where prophet row was missing
    combined = combined.withColumn(
        "split",
        when(
            col("split").isNull(),
            when(col("week") < to_date(lit(TEST_START)), lit("train")).otherwise(lit("test")),
        ).otherwise(col("split")),
    )

    return combined


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("Building forecast results table (MA + Prophet)...")
    results_df = build_forecast_results(spark)

    print(f"  Total rows: {results_df.count()}")
    test_df = results_df.filter(col("split") == "test")
    print(f"  Test rows:  {test_df.count()}")

    print("\nWriting gold/forecast_results...")
    results_df.write.mode("overwrite").parquet(GOLD_FORECAST_RESULTS)

    print("Computing accuracy KPIs...")
    test_pd      = test_df.toPandas()
    accuracy_pd  = build_accuracy_summary(test_pd)

    print("\nAccuracy summary (WMAPE by model):")
    summary = (
        accuracy_pd
        .groupby("model")[["rmse", "mae", "wmape", "bias"]]
        .mean()
        .round(4)
    )
    print(summary.to_string())

    print("\nWriting gold/forecast_accuracy...")
    spark_accuracy = spark.createDataFrame(accuracy_pd)
    # Convert NaN → NULL so DuckDB aggregations work correctly
    for float_col in ["rmse", "mae", "wmape", "bias"]:
        spark_accuracy = spark_accuracy.withColumn(
            float_col, nanvl(col(float_col), lit(None).cast("double"))
        )
    spark_accuracy.write.mode("overwrite").parquet(GOLD_FORECAST_ACCURACY)

    spark.stop()
    print("\nGold forecast tables complete.")
