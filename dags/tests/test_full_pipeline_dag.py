import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_dag_imports_and_has_expected_tasks():
    import full_pipeline_dag

    dag = full_pipeline_dag.dag
    task_ids = set(dag.task_ids)
    assert task_ids == {
        "resolve_corpus",
        "wait_for_corpus_size",
        "normalize_and_extract",
        "route_after_normalize",
        "sciparse_convert",
        "corpus_ready",
        "shared_corpus_prep",
        "select_branches",
        "branch_a_finetune",
        "branch_b_finetune",
        "branch_c_lora",
        "branch_cbow",
    }


def test_dag_has_max_active_runs_one():
    import full_pipeline_dag

    assert full_pipeline_dag.dag.max_active_runs == 1


def test_dag_requires_lang_param():
    import full_pipeline_dag

    assert "lang" in full_pipeline_dag.dag.params
