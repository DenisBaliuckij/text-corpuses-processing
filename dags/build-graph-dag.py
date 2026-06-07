# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="build_graph",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def build_graph():
        import io
        import json
        from repositories.graph_job_repository import GraphJobRepository
        import ftpConnector
        from ftpConnector import ftpConnector
        import graphBuilder
        from graphBuilder import extract_graph_edges, merge_graph

        file_row = GraphJobRepository.get_file_for_graph_building()
        if file_row is None:
            return

        file_id = file_row[0]
        resolved_path = file_row[1]
        job_id = file_row[2]

        try:
            GraphJobRepository.transition_to_execution(job_id)

            resolved_file = ftpConnector.getFile(resolved_path, 'Graph')
            resolved_file.seek(0)
            text = resolved_file.read().decode('utf-8', errors='replace')

            new_edges = extract_graph_edges(text)

            graph_path = f"graphJobs/{job_id}/graph.json"
            try:
                existing_file = ftpConnector.getFile(graph_path, 'Graph')
                existing_file.seek(0)
                graph = json.loads(existing_file.read().decode('utf-8'))
            except Exception:
                graph = {"nodes": [], "edges": []}

            graph = merge_graph(graph, new_edges)

            graph_bytes = json.dumps(graph, ensure_ascii=False).encode('utf-8')
            ftpConnector.storeFile(graph_path, io.BytesIO(graph_bytes), 'Graph')

            GraphJobRepository.mark_graph_done(file_id)
        except Exception as e:
            GraphJobRepository.set_file_error(file_id, str(e))

    build_graph()
