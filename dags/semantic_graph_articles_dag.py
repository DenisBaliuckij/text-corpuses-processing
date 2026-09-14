"""Manually-triggered DAG: build a per-article semantic graph (phrase-level
semantic units, not whole sentences) over N randomly-sampled arxiv .tex
articles. Follows the same SSHOperator + docker_run_watchdog.sh pattern as
full_pipeline_dag.py -- every compute step runs against langembed-ml on
corpus-host. See langembed repo's src/langembed/data/semantic_units.py for
the chunking logic and scripts/{extract_article_sentences,
chunk_article_sentences, embed_article_units, build_article_graphs}.py for
the pipeline stages.
"""

import pendulum

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import DAG, Param

SSH_CONN_ID = "corpus_host_ssh"
LANGEMBED_BASE = "/home/s939/langembed_deploy/langembed"
WATCHDOG = f"{LANGEMBED_BASE}/scripts/docker_run_watchdog.sh"

with DAG(
    dag_id="semantic_graph_articles",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 14, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["langembed", "manual", "semantic-graph"],
    params={
        "tex_dir": Param(default="/opt/latex/arxiv", type="string"),
        "n_articles": Param(default=10, type="integer"),
        "seed": Param(default=42, type="integer"),
        "max_sentences_per_article": Param(default=120, type="integer"),
        "min_sentence_chars": Param(default=25, type="integer"),
        "method": Param(default="spacy", enum=["spacy", "regex"]),
        "spacy_model": Param(default="en_core_web_sm", type="string"),
        "embed_model_dir": Param(default="artifacts/simcse_en", type="string"),
        "embed_batch_size": Param(default=32, type="integer"),
        "k": Param(default=5, type="integer"),
        "min_cluster_size": Param(default=8, type="integer"),
        "timeout_extract_minutes": Param(default=15, type="integer"),
        "timeout_chunk_minutes": Param(default=30, type="integer"),
        "timeout_embed_minutes": Param(default=30, type="integer"),
        "timeout_graph_minutes": Param(default=15, type="integer"),
    },
) as dag:
    # CPU-only: pure text parsing, no ML.
    extract_sentences = SSHOperator(
        task_id="extract_sentences",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        # Runs directly on the host (not via docker_run_watchdog.sh/langembed-ml):
        # pure text parsing, no ML deps, and needs /opt/latex/arxiv which the
        # watchdog's container mounts don't include -- same as the original
        # semantic_graph/ prototype's "Stage 1 (host, no ML deps)" design.
        command=(
            f"cd {LANGEMBED_BASE} && timeout "
            "{{ (params.timeout_extract_minutes * 60) | int }} python3 "
            "scripts/extract_article_sentences.py "
            "--tex-dir {{ params.tex_dir }} "
            "--n-articles {{ params.n_articles }} --seed {{ params.seed }} "
            "--max-sentences-per-article {{ params.max_sentences_per_article }} "
            "--min-sentence-chars {{ params.min_sentence_chars }} "
            "--out data/article_sentences_en.jsonl "
            "--manifest-out data/article_sample_manifest.json"
        ),
    )

    # CPU-only: spaCy parser, no GPU needed for phrase chunking.
    chunk_sentences = SSHOperator(
        task_id="chunk_sentences",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}-chunk "
            "{{ params.timeout_chunk_minutes }} false "
            "scripts/chunk_article_sentences.py "
            "--in data/article_sentences_en.jsonl "
            "--out data/article_semantic_units_en.jsonl "
            "--method {{ params.method }} --spacy-model {{ params.spacy_model }}"
        ),
    )

    # GPU: loads the SentenceTransformer branch-A model, same as branch tasks.
    embed_units = SSHOperator(
        task_id="embed_units",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}-embed "
            "{{ params.timeout_embed_minutes }} true "
            "scripts/embed_article_units.py "
            "--in data/article_semantic_units_en.jsonl "
            "--out output/en/article_unit_embeddings.jsonl "
            "--model-dir {{ params.embed_model_dir }} "
            "--batch-size {{ params.embed_batch_size }}"
        ),
    )

    # CPU-only: networkx k-NN + greedy-modularity clustering, no GPU.
    build_graph = SSHOperator(
        task_id="build_graph",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}-graph "
            "{{ params.timeout_graph_minutes }} false "
            "scripts/build_article_graphs.py "
            "--in output/en/article_unit_embeddings.jsonl "
            "--out output/en/article_graphs_data.json "
            "--k {{ params.k }} --min-cluster-size {{ params.min_cluster_size }}"
        ),
    )

    extract_sentences >> chunk_sentences >> embed_units >> build_graph
