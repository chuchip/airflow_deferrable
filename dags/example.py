from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
# Import your custom operator (Airflow finds it via the plugin mechanism)
from http_polling_plugin.operators.http_polling_operator import HttpPollingDeferrableOperator

# Define your HTTP connection in Airflow UI, e.g., 'my_api_conn'
# with base URL like 'https://api.example.com'

with DAG(
    dag_id="deferrable_http_polling_example",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["deferrable", "http", "example"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Assume an initial call starts a pro:cess (using SimpleHttpOperator or similar)
    # ... start_process_task ...

    wait_for_completion = HttpPollingDeferrableOperator(
        task_id="wait_for_api_completion",
        http_conn_id="my_api_connection",        # Your Airflow HTTP connection ID        
        endpoint="http://localhost:8080/api/v1/process/status/12345", # Templatable endpoint
        method="GET",
        response_field="job_status",      # Check the 'job_status' field in the JSON
        success_value="COMPLETED",        # Success if job_status == 'COMPLETED'
        failure_values=["FAILED", "ABORTED"], # Fail if job_status is FAILED or ABORTED
        poke_interval=60,                 # Check every 60 seconds
        http_check_retries=2,             # Retry a failed HTTP GET 2 times (total 3 attempts)
        retry_delay=10.0,                 # Wait 10s between failed HTTP GET retries
        # headers={"Authorization": "Bearer {{ conn.my_api_conn.password }}"} # Example templated header
    )

    end = EmptyOperator(task_id="end")

    # Define dependencies
    start >> wait_for_completion >> end
    # start >> start_process_task >> wait_for_completion >> end # More realistic flow