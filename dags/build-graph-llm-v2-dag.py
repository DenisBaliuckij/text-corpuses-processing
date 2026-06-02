# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="build_graph_llm_v2",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["treeFormation"],
) as dag:

    @task()
    def build_graph_llm_v2():
        """
        Picks one anaphora-resolved text file (GraphConstructionFiles.Status=10),
        runs the llm_v2 LLM pipeline (local HuggingFace model), saves raw_graph.json
        and clustered_graph.json to FTP under graphJobs/{jobId}/llm_v2/{fileId}/,
        and marks the file done (Status=20).

        Pipeline stages: preprocessing -> coreference -> chunking -> extraction
          -> normalization -> deduplication -> graph assembly -> clustering.

        LLM config defaults to Qwen2-1.5B-Instruct on CPU. Override via
        GraphConstructionJob.ProcessorConfig JSON: {"processorName": "LLMv2",
        "llm": {"model_name": "...", "device": "cuda"}, "embedding": {...}}.
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

            from llm_v2.config_schema import PipelineConfig
            from llm_v2.pipeline import Pipeline

            config = PipelineConfig(
                paths={"input_text": "", "output_dir": tempfile.mkdtemp()}
            )

            base_dir = Path(dag_folder) / 'llm_v2'
            pipe = Pipeline(config, base_dir=base_dir)
            result = pipe.run(text=text)

            base_path = f"graphJobs/{job_id}/llm_v2/{file_id}"

            raw_bytes = json.dumps(
                result["raw_graph"].model_dump(), ensure_ascii=False
            ).encode('utf-8')
            ftpConnector.storeFile(f"{base_path}/raw_graph.json", io.BytesIO(raw_bytes), 'Graph')

            clustered_bytes = json.dumps(
                result["clustered_graph"].model_dump(), ensure_ascii=False
            ).encode('utf-8')
            ftpConnector.storeFile(f"{base_path}/clustered_graph.json", io.BytesIO(clustered_bytes), 'Graph')

            GraphJobRepository.mark_graph_done(file_id)

        except Exception as e:
            GraphJobRepository.set_file_error(file_id, str(e))

    build_graph_llm_v2()
