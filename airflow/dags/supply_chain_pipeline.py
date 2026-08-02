from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/usr/local/airflow/project"

default_args = {
    "owner": "nanda",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="supply_chain_pipeline",
    description="Bronze → Silver → Gold medallion pipeline for DataCo supply chain data",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["supply-chain", "lakehouse"],
) as dag:

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"cd {PROJECT_DIR} && python src/bronze/ingest_supply_chain.py",
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=f"cd {PROJECT_DIR} && python src/silver/transform_supply_chain.py",
    )

    gold_delivery_performance = BashOperator(
        task_id="gold_delivery_performance",
        bash_command=f"cd {PROJECT_DIR} && python src/gold/delivery_performance.py",
    )

    gold_supplier_performance = BashOperator(
        task_id="gold_supplier_performance",
        bash_command=f"cd {PROJECT_DIR} && python src/gold/supplier_performance.py",
    )

    gold_shipping_analysis = BashOperator(
        task_id="gold_shipping_analysis",
        bash_command=f"cd {PROJECT_DIR} && python src/gold/shipping_analysis.py",
    )

    gold_forecast_accuracy = BashOperator(
        task_id="gold_forecast_accuracy",
        bash_command=f"cd {PROJECT_DIR} && python src/gold/forecast_accuracy.py",
    )

    bronze_ingest >> silver_transform >> [
        gold_delivery_performance,
        gold_supplier_performance,
        gold_shipping_analysis,
        gold_forecast_accuracy,
    ]
