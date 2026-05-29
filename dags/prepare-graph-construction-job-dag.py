# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="prepare_graph_construction_job",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def prepare_job():
        import dbConnector
        from dbConnector import databaseConnector
        import ftpConnector
        from ftpConnector import ftpConnector

        job = databaseConnector.getJobForPreparation()
        if job is None:
            return

        job_id = job[0]
        included_paths = job[2]

        try:
            paths = [p.strip() for p in included_paths.split(';') if p.strip()]
            for path in paths:
                file_list = ftpConnector.getFileList(path, 'Tex')
                for file_path in file_list:
                    databaseConnector.addFileSourceForGraphConstructionJob(file_path, job_id)

            databaseConnector.processGraphCreationJobToTextCopying(job_id)
        except Exception as e:
            databaseConnector.setErrorForPreparationJob(job_id, str(e))

    prepare_job()
