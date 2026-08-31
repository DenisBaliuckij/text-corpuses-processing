import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest


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


def test_resolve_corpus_rejects_invalid_lang():
    import full_pipeline_dag

    with pytest.raises(ValueError, match="lang"):
        full_pipeline_dag.resolve_corpus.function(
            params={
                "lang": "not a lang!", "branches": ["A"], "source_mode": "existing_text",
                "source_documents": [], "raw_text_path": "", "base_model_b": "",
            }
        )


def test_resolve_corpus_rejects_invalid_branches():
    import full_pipeline_dag

    with pytest.raises(ValueError, match="branches"):
        full_pipeline_dag.resolve_corpus.function(
            params={
                "lang": "mr", "branches": ["Z"], "source_mode": "existing_text",
                "source_documents": [], "raw_text_path": "", "base_model_b": "",
            }
        )


def test_resolve_corpus_rejects_unsafe_raw_text_path():
    import full_pipeline_dag

    with pytest.raises(ValueError, match="raw_text_path"):
        full_pipeline_dag.resolve_corpus.function(
            params={
                "lang": "mr", "branches": ["A"], "source_mode": "existing_text",
                "source_documents": [], "raw_text_path": "data/raw/mr.txt; rm -rf /",
                "base_model_b": "",
            }
        )


def test_select_branches_maps_correctly():
    import full_pipeline_dag

    result = full_pipeline_dag.select_branches.function(params={"branches": ["A", "CBOW"]})
    assert result == ["branch_a_finetune", "branch_cbow"]


class _FakeTI:
    def xcom_pull(self, task_ids=None):
        return ["data/raw/mr_nllb.txt"]


def _render_shared_corpus_prep(no_clean: bool) -> str:
    import copy

    import full_pipeline_dag

    # render_template_fields mutates the task in place and is irreversible, so work on
    # a copy each time -- the DAG's real task is a shared module-level object and other
    # tests in this file (or a later run of this same helper) would otherwise see an
    # already-rendered (no-op on re-render) command instead of a freshly templated one.
    task = copy.deepcopy(full_pipeline_dag.dag.get_task("shared_corpus_prep"))
    ctx = {
        "params": {
            "no_clean": no_clean, "lang": "mr", "use_gpu": True,
            "timeout_corpus_prep_minutes": 240, "label_method": "svd", "embed_sample_size": 200,
        },
        "dag_run": {"run_id": "manual__2026-08-31T12:34:56.789+00:00"},
        "ti": _FakeTI(),
    }
    task.render_template_fields(ctx)
    return task.command


def test_shared_corpus_prep_renders_rm_rf_when_no_clean_false():
    command = _render_shared_corpus_prep(no_clean=False)
    assert command.startswith("rm -rf /home/s939/langembed_deploy/langembed/output/mr && ")


def test_shared_corpus_prep_skips_rm_rf_when_no_clean_true():
    command = _render_shared_corpus_prep(no_clean=True)
    assert "rm -rf" not in command


def test_shared_corpus_prep_sanitizes_run_id_for_docker_name():
    command = _render_shared_corpus_prep(no_clean=False)
    assert "manual__2026-08-31T12-34-56.789-00-00-corpus-prep" in command
    assert ":" not in command.split("docker_run_watchdog.sh ")[1].split(" ")[0]
    assert "+" not in command.split("docker_run_watchdog.sh ")[1].split(" ")[0]
