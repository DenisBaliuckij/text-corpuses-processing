# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="finalize_job",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def finalize_job():
        import dbConnector
        from dbConnector import databaseConnector

        result = databaseConnector.finalizeCompletedJobs()
        if result is not None:
            print(f"Finalized job ID: {result[0]}")

    finalize_job()
