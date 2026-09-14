import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_dag_imports_and_has_expected_tasks():
    import semantic_graph_articles_dag

    dag = semantic_graph_articles_dag.dag
    assert set(dag.task_ids) == {
        "extract_sentences",
        "chunk_sentences",
        "embed_units",
        "build_graph",
    }


def test_dag_has_max_active_runs_one():
    import semantic_graph_articles_dag

    assert semantic_graph_articles_dag.dag.max_active_runs == 1


def test_dag_tasks_run_in_sequence():
    import semantic_graph_articles_dag

    dag = semantic_graph_articles_dag.dag
    extract = dag.get_task("extract_sentences")
    chunk = dag.get_task("chunk_sentences")
    embed = dag.get_task("embed_units")
    build = dag.get_task("build_graph")

    assert chunk.task_id in [t.task_id for t in extract.downstream_list]
    assert embed.task_id in [t.task_id for t in chunk.downstream_list]
    assert build.task_id in [t.task_id for t in embed.downstream_list]


def test_dag_default_params_match_original_ten_article_sample():
    import semantic_graph_articles_dag

    params = semantic_graph_articles_dag.dag.params
    assert params["n_articles"].value == 10
    assert params["seed"].value == 42
    assert params["method"].value == "spacy"


def test_only_embed_units_uses_gpu():
    import semantic_graph_articles_dag

    for task_id in ("chunk_sentences", "build_graph"):
        command = semantic_graph_articles_dag.dag.get_task(task_id).command
        assert " false scripts/" in command, f"{task_id} should run CPU-only"

    embed_command = semantic_graph_articles_dag.dag.get_task("embed_units").command
    assert " true scripts/" in embed_command


def test_extract_sentences_runs_directly_on_host_not_via_watchdog():
    """extract_article_sentences.py needs /opt/latex/arxiv, which
    docker_run_watchdog.sh's container mounts don't include -- it must run
    as a plain host command, not through the watchdog/langembed-ml wrapper
    the other three tasks use."""
    import semantic_graph_articles_dag

    command = semantic_graph_articles_dag.dag.get_task("extract_sentences").command
    assert "docker_run_watchdog.sh" not in command
    assert "python3 scripts/extract_article_sentences.py" in command
