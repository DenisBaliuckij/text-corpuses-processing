"""Manually-triggered full-pipeline DAG: corpus (existing text or converted documents)
-> embeddings (branches A/B/C/CBOW) -> LLM training (branch C's LoRA). See
docs/superpowers/specs/2026-08-31-full-pipeline-dag-design.md (langembed repo) for the
full design.
"""

import pendulum

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import DAG, Param, task

SSH_CONN_ID = "corpus_host_ssh"
LANGEMBED_BASE = "/home/s939/langembed_deploy/langembed"
WATCHDOG = f"{LANGEMBED_BASE}/scripts/docker_run_watchdog.sh"

_BRANCH_TASK_IDS = {
    "A": "branch_a_finetune",
    "B": "branch_b_finetune",
    "C": "branch_c_lora",
    "CBOW": "branch_cbow",
}

with DAG(
    dag_id="full_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 31, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["langembed", "manual"],
    params={
        "lang": Param(default="", type="string"),
        "source_mode": Param(default="existing_text", enum=["existing_text", "convert_documents"]),
        "raw_text_path": Param(default="", type="string"),
        "source_documents": Param(default=[], type="array"),
        "conversion_method": Param(default="fast", enum=["fast", "sciparse"]),
        "min_corpus_size_mb": Param(default=50, type="integer"),
        "label_method": Param(default="svd", enum=["svd", "backtranslation", "native"]),
        "branches": Param(default=["A", "B", "C", "CBOW"], type="array"),
        "embed_sample_size": Param(default=200, type="integer"),
        "base_model_b": Param(default="sentence-transformers/LaBSE", type="string"),
        "use_gpu": Param(default=True, type="boolean"),
        "no_clean": Param(default=False, type="boolean"),
        "timeout_conversion_minutes": Param(default=60, type="integer"),
        "timeout_corpus_prep_minutes": Param(default=240, type="integer"),
        "timeout_branch_minutes": Param(default=480, type="integer"),
    },
) as dag:

    @task.branch()
    def resolve_corpus(**context) -> str:
        params = context["params"]
        if not params["lang"]:
            raise ValueError("`lang` is required")
        if not params["branches"]:
            raise ValueError("`branches` must select at least one of A/B/C/CBOW")
        if params["source_mode"] == "convert_documents" and not params["source_documents"]:
            raise ValueError("`source_documents` is required when source_mode=convert_documents")
        return "wait_for_corpus_size" if params["source_mode"] == "existing_text" else "normalize_and_extract"

    @task()
    def wait_for_corpus_size(**context) -> list[str]:
        import time

        from airflow.providers.ssh.hooks.ssh import SSHHook

        params = context["params"]
        raw_text_path = params["raw_text_path"] or f"data/raw/{params['lang']}_nllb.txt"
        min_bytes = params["min_corpus_size_mb"] * 1024 * 1024
        full_path = f"{LANGEMBED_BASE}/{raw_text_path}"

        hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            with hook.get_conn() as client:
                _, stdout, _ = client.exec_command(f"stat -c%s {full_path} 2>/dev/null || echo 0")
                size = int(stdout.read().decode().strip() or "0")
            if size >= min_bytes:
                return [raw_text_path]
            time.sleep(15)
        raise TimeoutError(f"{full_path} did not reach {params['min_corpus_size_mb']}MB within 600s")

    normalize_and_extract = SSHOperator(
        task_id="normalize_and_extract",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id }}}}-normalize "
            "{{ params.timeout_conversion_minutes }} false "
            "scripts/bridge_corpus.py --lang {{ params.lang }} "
            "--conversion-method {{ params.conversion_method }} "
            "--source-documents {{ params.source_documents | join(' ') }} "
            "--result-json /tmp/{{ dag_run.run_id }}_bridge_result.json"
        ),
    )

    @task.branch()
    def route_after_normalize(**context) -> str:
        return "sciparse_convert" if context["params"]["conversion_method"] == "sciparse" else "corpus_ready"

    @task()
    def sciparse_convert(**context) -> list[str]:
        """Runs natively in the worker (not via SSH) -- see sciparse_bridge.py's
        module docstring for why."""
        import json
        import subprocess
        from pathlib import Path

        params = context["params"]
        run_id = context["dag_run"].run_id
        result_path = Path(f"/tmp/{run_id}_bridge_result.json")
        subprocess.run(["scp", f"corpus_host:{result_path}", str(result_path)], check=True)
        container_pdf_paths = json.loads(result_path.read_text(encoding="utf-8"))["normalized_pdf_paths"]

        from sciparse_bridge import register_and_wait

        raw_paths = []
        for i, container_path in enumerate(container_pdf_paths):
            # normalize_and_extract ran inside a langembed-ml container with
            # `-v LANGEMBED_BASE:/app` (see docker_run_watchdog.sh) -- container-internal
            # paths under /app are the SAME bytes as LANGEMBED_BASE on the host, since
            # it's a bind mount, not ephemeral container storage. Translate the path
            # and scp the actual PDF down to the worker, which has no filesystem in
            # common with either the host or that (already-removed) container.
            host_path = container_path.replace("/app", LANGEMBED_BASE, 1)
            local_pdf = Path(f"/tmp/{run_id}_sciparse_src_{i}.pdf")
            subprocess.run(["scp", f"corpus_host:{host_path}", str(local_pdf)], check=True)

            text = register_and_wait(
                local_pdf, params["lang"], timeout_s=params["timeout_conversion_minutes"] * 60
            )
            out_path = f"/tmp/{run_id}_sciparse_{i}.txt"
            Path(out_path).write_text(text, encoding="utf-8")
            raw_paths.append(out_path)
        return raw_paths

    @task(trigger_rule="none_failed_min_one_success")
    def corpus_ready(**context) -> list[str]:
        """Regardless of which upstream branch actually ran (existing_text,
        convert_documents+fast, or convert_documents+sciparse), figures out the
        resulting raw-text file paths to feed shared_corpus_prep."""
        import json
        import subprocess
        from pathlib import Path

        params = context["params"]
        ti = context["ti"]
        run_id = context["dag_run"].run_id

        if params["source_mode"] == "existing_text":
            return ti.xcom_pull(task_ids="wait_for_corpus_size")

        if params["conversion_method"] == "sciparse":
            return ti.xcom_pull(task_ids="sciparse_convert")

        result_path = Path(f"/tmp/{run_id}_bridge_result.json")
        subprocess.run(["scp", f"corpus_host:{result_path}", str(result_path)], check=True)
        return json.loads(result_path.read_text(encoding="utf-8"))["raw_text_paths"]

    shared_corpus_prep = SSHOperator(
        task_id="shared_corpus_prep",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            "{{ '' if params.no_clean else 'rm -rf " + LANGEMBED_BASE + "/output/' ~ params.lang ~ ' && ' }}"
            f"{WATCHDOG} {{{{ dag_run.run_id }}}}-corpus-prep "
            "{{ params.timeout_corpus_prep_minutes }} {{ params.use_gpu | lower }} "
            "scripts/run_pipeline.py --lang {{ params.lang }} "
            "--raw-input {{ ti.xcom_pull(task_ids='corpus_ready') | join(' ') }} "
            "--auto-label --auto-label-method {{ params.label_method }} "
            "--embed-sample-size {{ params.embed_sample_size }}"
        ),
    )

    @task.branch()
    def select_branches(**context) -> list[str]:
        return [_BRANCH_TASK_IDS[b] for b in context["params"]["branches"]]

    branch_a_finetune = SSHOperator(
        task_id="branch_a_finetune",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id }}}}-branch-a "
            "{{ params.timeout_branch_minutes }} {{ params.use_gpu | lower }} "
            "scripts/supervised_finetune_pass.py --lang {{ params.lang }} "
            "--label-method {{ params.label_method }}"
        ),
    )

    branch_b_finetune = SSHOperator(
        task_id="branch_b_finetune",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id }}}}-branch-b "
            "{{ params.timeout_branch_minutes }} {{ params.use_gpu | lower }} "
            "scripts/supervised_finetune_pass.py --lang {{ params.lang }} "
            "--label-method {{ params.label_method }} "
            "--base-model {{ params.base_model_b }} --out-tag b_mling"
        ),
    )

    branch_c_lora = SSHOperator(
        task_id="branch_c_lora",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id }}}}-branch-c "
            "{{ params.timeout_branch_minutes }} {{ params.use_gpu | lower }} "
            "scripts/embed_branch_c.py --lang {{ params.lang }} "
            "--label-method {{ params.label_method }} "
            "--embed-sample-size {{ params.embed_sample_size }}"
        ),
    )

    branch_cbow = SSHOperator(
        task_id="branch_cbow",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id }}}}-branch-cbow "
            "{{ params.timeout_branch_minutes }} false "
            "scripts/embed_branch_cbow.py --lang {{ params.lang }} "
            "--embed-sample-size {{ params.embed_sample_size }}"
        ),
    )

    corpus_branch = resolve_corpus()
    existing_result = wait_for_corpus_size()
    corpus_branch >> existing_result
    corpus_branch >> normalize_and_extract

    route_result = route_after_normalize()
    normalize_and_extract >> route_result

    sciparse_result = sciparse_convert()
    route_result >> sciparse_result

    ready = corpus_ready()
    existing_result >> ready
    route_result >> ready
    sciparse_result >> ready

    ready >> shared_corpus_prep

    branch_selection = select_branches()
    shared_corpus_prep >> branch_selection
    branch_selection >> [branch_a_finetune, branch_b_finetune, branch_c_lora, branch_cbow]
