# -*- coding: utf-8 -*-
import io
import json
import logging
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task
from repositories.graph_job_repository import GraphJobRepository
from ftpConnector import ftpConnector
from graphMetrics import compute_metrics
from graphVisualizer import generate_visualization


def _process_rulebased(job_id):
    graph_path = f"graphJobs/{job_id}/graph.json"
    raw = ftpConnector.getFile(graph_path, 'Graph')
    raw.seek(0)
    graph_dict = json.loads(raw.read().decode('utf-8'))

    metrics = compute_metrics(graph_dict, "RuleBased")
    ftpConnector.storeFile(
        f"graphJobs/{job_id}/metrics.json",
        io.BytesIO(json.dumps(metrics, indent=2).encode('utf-8')),
        'Graph'
    )
    html = generate_visualization(graph_dict, "RuleBased")
    ftpConnector.storeFile(
        f"graphJobs/{job_id}/visualization.html",
        io.BytesIO(html.encode('utf-8')),
        'Graph'
    )


def _process_per_file(job_id, file_id, backend):
    prefix = "llm_v2" if backend == "LLMv2" else "hierarchical"
    base = f"graphJobs/{job_id}/{prefix}/{file_id}"

    raw = ftpConnector.getFile(f"{base}/clustered_graph.json", 'Graph')
    raw.seek(0)
    graph_dict = json.loads(raw.read().decode('utf-8'))

    metrics = compute_metrics(graph_dict, backend)
    ftpConnector.storeFile(
        f"{base}/metrics.json",
        io.BytesIO(json.dumps(metrics, indent=2).encode('utf-8')),
        'Graph'
    )
    html = generate_visualization(graph_dict, backend)
    ftpConnector.storeFile(
        f"{base}/visualization.html",
        io.BytesIO(html.encode('utf-8')),
        'Graph'
    )


with DAG(
    dag_id="finalize_job",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def finalize_job():
        result = GraphJobRepository.finalize_completed_jobs()
        if result is None:
            return

        job_id = result[0]
        print(f"Finalized job ID: {job_id}")

        config_json = GraphJobRepository.get_processor_config(job_id)
        config = json.loads(config_json) if config_json else {}
        processor = config.get("processorName", "RuleBased")

        try:
            if processor == "RuleBased":
                _process_rulebased(job_id)
            else:
                backend = "Hierarchical" if processor == "Hierarchical" else "LLMv2"
                file_rows = GraphJobRepository.get_files_for_job(job_id)
                for row in file_rows:
                    _process_per_file(job_id, row[0], backend)
        except Exception as e:
            logging.error(f"Metrics/visualization generation failed for job {job_id}: {e}")

    finalize_job()
