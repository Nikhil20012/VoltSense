"""
Daily pipeline: transform, test, train, predict, refresh.
If dbt tests fail, downstream tasks don't run.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt/voltsense_dbt"
ML_DIR = "/opt/airflow/ml"

default_args = {
    "owner": "voltsense",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="voltsense_daily_pipeline",
    default_args=default_args,
    description="Daily: dbt run, test, train, predict, refresh Power BI",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["voltsense"],
) as dag:

    check_raw = BashOperator(
        task_id="check_raw_freshness",
        bash_command=(
            "python -c \""
            "import snowflake.connector, os; "
            "conn = snowflake.connector.connect("
            "account=os.environ['SNOWFLAKE_ACCOUNT'], "
            "user=os.environ['SNOWFLAKE_USER'], "
            "password=os.environ['SNOWFLAKE_PASSWORD'], "
            "database='VOLTSENSE', warehouse='VOLTSENSE_WH'); "
            "cur = conn.cursor(); "
            "cur.execute('SELECT COUNT(*) FROM RAW.RAW_CHARGER_SESSIONS "
            "WHERE RECORD_METADATA:CreateTime::timestamp_ntz > DATEADD(hour, -6, CURRENT_TIMESTAMP())'); "
            "count = cur.fetchone()[0]; "
            "assert count > 0, f'No fresh data in raw tables. Count: {count}'; "
            "print(f'Raw freshness check passed: {count} recent rows'); "
            "conn.close()\""
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command=f"cd {ML_DIR}/.. && python ml/train.py",
    )

    write_predictions = BashOperator(
        task_id="write_predictions",
        bash_command=f"cd {ML_DIR}/.. && python ml/predict.py",
    )

    refresh_pbi = BashOperator(
        task_id="refresh_power_bi",
        bash_command="echo 'Power BI refresh placeholder'",
    )

    check_raw >> dbt_run >> dbt_test >> train_model >> write_predictions >> refresh_pbi