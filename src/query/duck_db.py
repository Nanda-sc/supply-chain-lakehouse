import os
import duckdb
import pandas as pd

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import (
    GOLD_DELIVERY_PERFORMANCE,
    GOLD_SUPPLIER_PERFORMANCE,
    GOLD_SHIPPING_ANALYSIS,
)


def get_connection():
    return duckdb.connect()


def get_delivery_performance() -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT *
        FROM read_parquet('{GOLD_DELIVERY_PERFORMANCE}/*.parquet')
        ORDER BY market, order_region
    """).df()


def get_supplier_performance() -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT *
        FROM read_parquet('{GOLD_SUPPLIER_PERFORMANCE}/*.parquet')
        ORDER BY total_revenue DESC
    """).df()


def get_shipping_analysis() -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT *
        FROM read_parquet('{GOLD_SHIPPING_ANALYSIS}/*.parquet')
        ORDER BY shipping_mode, market
    """).df()


def get_delivery_summary() -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT
            market,
            SUM(total_orders)     AS total_orders,
            SUM(late_deliveries)  AS late_deliveries,
            ROUND(SUM(late_deliveries) * 100.0 / SUM(total_orders), 2) AS late_rate_pct
        FROM read_parquet('{GOLD_DELIVERY_PERFORMANCE}/*.parquet')
        GROUP BY market
        ORDER BY late_rate_pct DESC
    """).df()


def get_top_categories(n: int = 10) -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT
            department_name,
            category_name,
            total_revenue,
            total_profit,
            profit_margin_pct
        FROM read_parquet('{GOLD_SUPPLIER_PERFORMANCE}/*.parquet')
        ORDER BY total_revenue DESC
        LIMIT {n}
    """).df()


def get_shipping_mode_summary() -> pd.DataFrame:
    con = get_connection()
    return con.execute(f"""
        SELECT
            shipping_mode,
            SUM(total_orders)                                                    AS total_orders,
            ROUND(AVG(avg_days_shipping_real), 2)                                AS avg_days_real,
            ROUND(AVG(avg_days_shipping_scheduled), 2)                           AS avg_days_scheduled,
            ROUND(SUM(late_orders) * 100.0 / SUM(total_orders), 2)              AS late_rate_pct,
            ROUND(SUM(on_time_orders) * 100.0 / SUM(total_orders), 2)           AS on_time_rate_pct
        FROM read_parquet('{GOLD_SHIPPING_ANALYSIS}/*.parquet')
        GROUP BY shipping_mode
        ORDER BY late_rate_pct DESC
    """).df()


if __name__ == "__main__":
    print("=== Delivery Summary by Market ===")
    print(get_delivery_summary().to_string(index=False))

    print("\n=== Top 10 Categories by Revenue ===")
    print(get_top_categories(10).to_string(index=False))

    print("\n=== Shipping Mode Summary ===")
    print(get_shipping_mode_summary().to_string(index=False))
