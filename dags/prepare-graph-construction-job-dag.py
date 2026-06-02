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
        from repositories.graph_job_repository import GraphJobRepository
        import ftpConnector
        from ftpConnector import ftpConnector

        job = GraphJobRepository.get_job_for_preparation()
        if job is None:
            return

        job_id = job[0]
        included_paths = job[2]

        try:
            paths = [p.strip() for p in included_paths.split(';') if p.strip()]
            for path in paths:
                file_list = ftpConnector.getFileList(path, 'Tex')
                for file_path in file_list:
                    GraphJobRepository.add_file_source(file_path, job_id)

            GraphJobRepository.process_to_text_copying(job_id)
        except Exception as e:
            GraphJobRepository.set_job_error(job_id, str(e))

    prepare_job()
