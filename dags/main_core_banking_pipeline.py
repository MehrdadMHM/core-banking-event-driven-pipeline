import time

import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)


@dag(
    dag_id="main_core_banking_pipeline",
    description="Run Core Banking extraction and processing pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=[
        "core-banking",
        "main-pipeline",
        "kafka",
        "orchestration",
    ],
)
def main_core_banking_pipeline_dag():

    extract_transactions = TriggerDagRunOperator(
        task_id="trigger_extract_transactions",
        trigger_dag_id="extract_core_transactions",
        wait_for_completion=False,
    )

    extract_payments = TriggerDagRunOperator(
        task_id="trigger_extract_payments",
        trigger_dag_id="extract_core_payments",
        wait_for_completion=False,
    )

    extract_credit = TriggerDagRunOperator(
        task_id="trigger_extract_credit",
        trigger_dag_id="extract_core_credit",
        wait_for_completion=False,
    )

    @task(task_id="wait_for_raw_events")
    def wait_for_raw_events():
        time.sleep(15)

    raw_events_ready = wait_for_raw_events()

    process_raw_events = TriggerDagRunOperator(
        task_id="trigger_process_raw_events",
        trigger_dag_id="process_kafka_raw_events",
        wait_for_completion=False,
    )

    [
        extract_transactions,
        extract_payments,
        extract_credit,
    ] >> raw_events_ready >> process_raw_events


main_core_banking_pipeline_dag()