"""
AtliQ Phase 2 — Streaming Ops DAG
Hourly: check_fresh_events -> optimize_tables -> refresh_daily_summary
"""
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/5cdc3cf12f608cc0"
CATALOG = "atliq_dbx"
SCHEMA = "streaming"

# How far back the data-quality gate looks for fresh events.
# Keep at 120 normally; set to 2 temporarily to demo the failure quickly.
DQ_WINDOW_MINUTES = 120

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retrsy_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["atliq", "phase2", "streaming"],
) as dag:

    # TODO 1 — Data-quality gate: fails if no events landed recently
    check_fresh_events = DatabricksSqlOperator(
        task_id="check_fresh_events",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=f"""
            SELECT assert_true(
                COUNT(*) > 0,
                'No fresh events landed in the last {DQ_WINDOW_MINUTES} minutes'
            )
            FROM {CATALOG}.{SCHEMA}.silver_order_events
            WHERE event_ts >= current_timestamp() - INTERVAL {DQ_WINDOW_MINUTES} MINUTES
        """,
    )

    # TODO 2 — Table maintenance: compact small files
    optimize_tables = DatabricksSqlOperator(
        task_id="optimize_tables",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=[
            f"OPTIMIZE {CATALOG}.{SCHEMA}.silver_order_events",
            f"OPTIMIZE {CATALOG}.{SCHEMA}.gold_revenue_5min",
        ],
    )

    # TODO 3 — Daily rollup
    refresh_daily_summary = DatabricksSqlOperator(
        task_id="refresh_daily_summary",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=f"""
            CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_daily_summary AS
            SELECT
                CAST(event_ts AS DATE)                              AS event_date,
                COUNT_IF(event_type = 'order_placed')               AS orders_placed,
                COUNT_IF(event_type = 'payment_received')           AS orders_paid,
                COUNT_IF(event_type = 'order_cancelled')            AS orders_cancelled,
                SUM(CASE WHEN event_type = 'payment_received'
                         THEN order_amount ELSE 0 END)              AS revenue
            FROM {CATALOG}.{SCHEMA}.silver_order_events
            GROUP BY CAST(event_ts AS DATE)
        """,
    )

    # TODO 4 — Chain in order
    check_fresh_events >> optimize_tables >> refresh_daily_summary