# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="build_graph_hierarchical",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def build_graph_hierarchical():
        """
        Picks one anaphora-resolved text file (GraphConstructionFiles.Status=10),
        runs the hierarchical LLM pipeline (Yandex Cloud OpenAI-compatible API),
        saves raw_graph.json, clustered_graph.json, and hierarchy_tree.json to FTP
        under graphJobs/{jobId}/hierarchical/{fileId}/, then marks the file done.

        Pipeline stages: preprocessing -> chunking -> pass1 (chunk summaries +
          concept hierarchy) -> pass2 (context-aware extraction) ->
          entity resolution -> graph assembly -> importance filtering / clustering.

        Requires env var YANDEX_CLOUD_API_KEY. LLM config defaults to
        deepseek-v32/latest on Yandex Cloud. Override via
        GraphConstructionJob.ProcessorConfig JSON:
        {"processorName": "Hierarchical", "llm": {"api_key": "...", "base_url": "..."}}.
        """
        import io
        import json
        import os
        import sys
        import tempfile
        from pathlib import Path

        dag_folder = os.path.dirname(os.path.abspath(__file__))
        if dag_folder not in sys.path:
            sys.path.insert(0, dag_folder)

        from repositories.graph_job_repository import GraphJobRepository
        import ftpConnector
        from ftpConnector import ftpConnector

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

            from hierarchical_llm_version.config_schema import PipelineConfig
            from hierarchical_llm_version.pipeline import Pipeline

            config = PipelineConfig(
                paths={"input_text": "", "output_dir": tempfile.mkdtemp()}
            )

            base_dir = Path(dag_folder) / 'hierarchical_llm_version'
            pipe = Pipeline(config, base_dir=base_dir)
            result = pipe.run(text=text)

            base_path = f"graphJobs/{job_id}/hierarchical/{file_id}"

            raw_bytes = json.dumps(
                result["raw_graph"].model_dump(), ensure_ascii=False
            ).encode('utf-8')
            ftpConnector.storeFile(f"{base_path}/raw_graph.json", io.BytesIO(raw_bytes), 'Graph')

            clustered_bytes = json.dumps(
                result["clustered_graph"].model_dump(), ensure_ascii=False
            ).encode('utf-8')
            ftpConnector.storeFile(f"{base_path}/clustered_graph.json", io.BytesIO(clustered_bytes), 'Graph')

            if result.get("hierarchy_tree") is not None:
                hierarchy_bytes = json.dumps(
                    result["hierarchy_tree"].model_dump(), ensure_ascii=False
                ).encode('utf-8')
                ftpConnector.storeFile(f"{base_path}/hierarchy_tree.json", io.BytesIO(hierarchy_bytes), 'Graph')

            GraphJobRepository.mark_graph_done(file_id)

        except Exception as e:
            GraphJobRepository.set_file_error(file_id, str(e))

    build_graph_hierarchical()
