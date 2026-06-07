# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="resolve_anaphora",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def resolve_anaphora():
        import io
        import json
        from repositories.graph_job_repository import GraphJobRepository
        import ftpConnector
        from ftpConnector import ftpConnector
        from anaphoraResolver import resolve_and_substitute

        file_row = GraphJobRepository.get_file_for_anaphora()
        if file_row is None:
            return

        file_id = file_row[0]
        file_path = file_row[1]
        job_id = file_row[2]

        try:
            config_json = GraphJobRepository.get_processor_config(job_id)
            config = json.loads(config_json) if config_json else {}
            resolver_name = config.get("anaphoraResolverName", "LapinLiass")

            raw_file = ftpConnector.getFile(file_path, 'Tex')
            raw_file.seek(0)
            text = raw_file.read().decode('utf-8', errors='replace')

            output, _, _ = resolve_and_substitute(text, resolver_name=resolver_name)

            resolved_path = f"graphJobs/{job_id}/anaphora/{file_id}.txt"
            ftpConnector.storeFile(
                resolved_path,
                io.BytesIO(output.encode('utf-8')),
                'Graph'
            )

            GraphJobRepository.mark_anaphora_done(file_id, resolved_path)
        except Exception as e:
            GraphJobRepository.set_file_error(file_id, str(e))

    resolve_anaphora()
