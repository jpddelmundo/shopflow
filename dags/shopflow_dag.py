import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


DEFAULT_ARGS = {
    "owner": "me",
    "retries": 5,
    "retry_interval": timedelta(seconds=10)
}

DAG_ID = "shopflow_dag"
WORKING_DIR = "/home/jpdelmundo/shopflow"


def _stage_data(**context):
    task_instance = context["ti"]
    ds = task_instance.xcom_pull(task_ids="ingest_data_from_sources", key="day")
    cmd = ["python3", f"{WORKING_DIR}/exec_sql.py {WORKING_DIR}/warehouse/tables/staging.sql"]
    subprocess.run(cmd)
    print(f"Staging data dated {repr(ds)} ready")

def _data_warehouse_refresh(**context):
    ti = context["ti"]
    cmd = ["python3", f"{WORKING_DIR}/exec_sql.py {WORKING_DIR}/warehouse/tables/warehouse.sql"]
    subprocess.run(cmd)


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 11, 9, tz="UTC"),
    schedule="@daily",
    catchup=False
):
    
    ingest = BashOperator(
        task_id="ingest_data_from_sources",
        bash_command=f"python3 {(os.path.join(WORKING_DIR, 'ingest.py'))} {{{{ ds }}}}",
        cwd=WORKING_DIR
    )

    stage = PythonOperator(
        task_id="stage_ingested_data_to_duckdb",
        python_callable=_stage_data
    )

    transform = BashOperator(
        task_id="transform_and_enrich",
        bash_command=f"python3 {(os.path.join(WORKING_DIR, 'spark_jobs/transform_data.py'))} {{{{ ds }}}}",
        cwd=WORKING_DIR
    )

    dw = PythonOperator(
        task_id="data_warehouse_refresh",
        python_callable=_data_warehouse_refresh
    )

    ingest >> [stage, transform] >> dw

    