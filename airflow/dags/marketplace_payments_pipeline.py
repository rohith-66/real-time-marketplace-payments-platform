from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

HOST_PROJECT_PATH = "/Users/rohithsrinivasa/Documents/real-time-marketplace-payments-platform"
CONTAINER_PROJECT_PATH = "/opt/project"

with DAG(
    dag_id="marketplace_payments_pipeline",
    start_date=datetime(2026, 3, 13),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "marketplace"],
) as dag:

    check_bronze_ready = BashOperator(
        task_id="check_bronze_ready",
        bash_command=f"""
        if [ ! -d "{CONTAINER_PROJECT_PATH}/data/bronze" ]; then
          echo "Bronze folder not found"
          exit 1
        fi
        echo "Bronze data exists"
        """,
    )

    run_silver_transform = BashOperator(
        task_id="run_silver_transform",
        bash_command=f"""
        docker run --rm \
          -v "{HOST_PROJECT_PATH}":/opt/spark/work-dir \
          -w /opt/spark/work-dir \
          apache/spark:3.5.1 \
          /opt/spark/bin/spark-submit \
          spark/silver_transform.py
        """,
    )

    run_gold_transform = BashOperator(
        task_id="run_gold_transform",
        bash_command=f"""
        docker run --rm \
          -v "{HOST_PROJECT_PATH}":/opt/spark/work-dir \
          -w /opt/spark/work-dir \
          apache/spark:3.5.1 \
          /opt/spark/bin/spark-submit \
          spark/gold_transform.py
        """,
    )

    validate_gold_outputs = BashOperator(
        task_id="validate_gold_outputs",
        bash_command=f"""
        if [ ! -d "{CONTAINER_PROJECT_PATH}/data/gold/seller_metrics" ]; then
          echo "seller_metrics missing"
          exit 1
        fi
        if [ ! -d "{CONTAINER_PROJECT_PATH}/data/gold/city_metrics" ]; then
          echo "city_metrics missing"
          exit 1
        fi
        if [ ! -d "{CONTAINER_PROJECT_PATH}/data/gold/platform_metrics" ]; then
          echo "platform_metrics missing"
          exit 1
        fi

        echo "Gold outputs validated"
        """,
    )

    check_bronze_ready >> run_silver_transform >> run_gold_transform >> validate_gold_outputs