"""Manually-triggered full-pipeline DAG: corpus (existing text or converted documents)
-> embeddings (branches A/B/C/CBOW) -> LLM training (branch C's LoRA). See
docs/superpowers/specs/2026-08-31-full-pipeline-dag-design.md (langembed repo) for the
full design.
"""

import re

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

_SAFE_PATH_RE = re.compile(r"^[A-Za-z0-9_./-]+$")


def _remote_dir_exists(sftp, path: str) -> bool:
    try:
        sftp.stat(path)
        return True
    except FileNotFoundError:
        return False


with DAG(
    dag_id="full_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 31, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["langembed", "manual"],
    params={
        "lang": Param(default="", type="string"),
        "source_mode": Param(default="existing_text", enum=["existing_text", "convert_documents"]),
        "raw_text_path": Param(default="", type="string"),
        "source_documents": Param(default=[], type="array"),
        "conversion_method": Param(default="fast", enum=["fast", "sciparse"]),
        "min_corpus_size_mb": Param(default=50, type="integer"),
        "label_method": Param(default="svd", enum=["svd", "backtranslation"]),
        "branches": Param(default=["A", "B", "C", "CBOW"], type="array"),
        "embed_sample_size": Param(default=200, type="integer"),
        "base_model_b": Param(default="sentence-transformers/LaBSE", type="string"),
        "use_gpu": Param(default=True, type="boolean"),
        "no_clean": Param(default=False, type="boolean"),
        "train_llm": Param(default=False, type="boolean"),
        "llm_minutes": Param(default=25.0, type="number"),
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
        if not re.fullmatch(r"[a-z]{2,8}", params["lang"]):
            raise ValueError(f"`lang` must be 2-8 lowercase letters, got: {params['lang']!r}")
        if not params["branches"]:
            raise ValueError("`branches` must select at least one of A/B/C/CBOW")
        valid_branches = {"A", "B", "C", "CBOW"}
        invalid = set(params["branches"]) - valid_branches
        if invalid:
            raise ValueError(f"`branches` contains invalid values: {invalid}, must be a subset of {valid_branches}")
        if params["source_mode"] == "convert_documents" and not params["source_documents"]:
            raise ValueError("`source_documents` is required when source_mode=convert_documents")
        if params["raw_text_path"] and not _SAFE_PATH_RE.fullmatch(params["raw_text_path"]):
            raise ValueError(f"`raw_text_path` contains unsafe characters: {params['raw_text_path']!r}")
        for doc in params["source_documents"]:
            if not _SAFE_PATH_RE.fullmatch(doc):
                raise ValueError(f"`source_documents` entry contains unsafe characters: {doc!r}")
        if params["base_model_b"] and not _SAFE_PATH_RE.fullmatch(params["base_model_b"]):
            raise ValueError(f"`base_model_b` contains unsafe characters: {params['base_model_b']!r}")
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

    # Deliberately CPU-only regardless of use_gpu -- format normalization/extraction
    # and CBOW word vectors don't use the GPU.
    normalize_and_extract = SSHOperator(
        task_id="normalize_and_extract",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}-normalize "
            "{{ params.timeout_conversion_minutes }} false "
            "scripts/bridge_corpus.py --lang {{ params.lang }} "
            "--conversion-method {{ params.conversion_method }} "
            "--source-documents {{ params.source_documents | join(' ') }} "
            f"--result-json {LANGEMBED_BASE}/{{{{ dag_run.run_id }}}}_bridge_result.json"
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
        from pathlib import Path

        from airflow.providers.ssh.hooks.ssh import SSHHook

        params = context["params"]
        run_id = context["dag_run"].run_id
        hook = SSHHook(ssh_conn_id=SSH_CONN_ID)

        local_result_json = Path(f"/tmp/{run_id}_bridge_result.json")
        remote_result_json = f"{LANGEMBED_BASE}/{run_id}_bridge_result.json"
        with hook.get_conn() as client, client.open_sftp() as sftp:
            sftp.get(remote_result_json, str(local_result_json))
        container_pdf_paths = json.loads(local_result_json.read_text(encoding="utf-8"))[
            "normalized_pdf_paths"
        ]

        from sciparse_bridge import register_and_wait

        raw_paths = []
        with hook.get_conn() as client, client.open_sftp() as sftp:
            for i, repo_relative_pdf_path in enumerate(container_pdf_paths):
                remote_pdf = f"{LANGEMBED_BASE}/{repo_relative_pdf_path}"
                local_pdf = Path(f"/tmp/{run_id}_sciparse_src_{i}.pdf")
                sftp.get(remote_pdf, str(local_pdf))

                text = register_and_wait(
                    local_pdf, params["lang"], timeout_s=params["timeout_conversion_minutes"] * 60
                )

                repo_relative_out = f"data/bridge_state/{run_id}_sciparse_{i}.txt"
                remote_out = f"{LANGEMBED_BASE}/{repo_relative_out}"
                local_out = Path(f"/tmp/{run_id}_sciparse_{i}.txt")
                local_out.write_text(text, encoding="utf-8")
                sftp.mkdir(f"{LANGEMBED_BASE}/data/bridge_state") if not _remote_dir_exists(
                    sftp, f"{LANGEMBED_BASE}/data/bridge_state"
                ) else None
                sftp.put(str(local_out), remote_out)
                raw_paths.append(repo_relative_out)
        return raw_paths

    @task(trigger_rule="none_failed_min_one_success")
    def corpus_ready(**context) -> list[str]:
        """Regardless of which upstream branch actually ran (existing_text,
        convert_documents+fast, or convert_documents+sciparse), figures out the
        resulting raw-text file paths to feed shared_corpus_prep."""
        import json
        from pathlib import Path

        from airflow.providers.ssh.hooks.ssh import SSHHook

        params = context["params"]
        ti = context["ti"]
        run_id = context["dag_run"].run_id

        if params["source_mode"] == "existing_text":
            return ti.xcom_pull(task_ids="wait_for_corpus_size")

        if params["conversion_method"] == "sciparse":
            return ti.xcom_pull(task_ids="sciparse_convert")

        local_result_json = Path(f"/tmp/{run_id}_bridge_result.json")
        remote_result_json = f"{LANGEMBED_BASE}/{run_id}_bridge_result.json"
        hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
        with hook.get_conn() as client, client.open_sftp() as sftp:
            sftp.get(remote_result_json, str(local_result_json))
        return json.loads(local_result_json.read_text(encoding="utf-8"))["raw_text_paths"]

    shared_corpus_prep = SSHOperator(
        task_id="shared_corpus_prep",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        command=(
            "{{ '' if params.no_clean else 'rm -rf " + LANGEMBED_BASE + "/output/' ~ params.lang ~ ' && ' }}"
            f"{WATCHDOG} {{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}-corpus-prep "
            "{{ params.timeout_corpus_prep_minutes }} {{ params.use_gpu | lower }} "
            "scripts/run_pipeline.py --lang {{ params.lang }} "
            "--raw-input {{ ti.xcom_pull(task_ids='corpus_ready') | join(' ') }} "
            "--auto-label --auto-label-method {{ params.label_method }} "
            "--embed-sample-size {{ params.embed_sample_size }} "
            "{{ '--train-llm --llm-minutes ' ~ params.llm_minutes if params.train_llm else '' }}"
        ),
    )

    @task.branch()
    def select_branches(**context) -> list[str]:
        return [_BRANCH_TASK_IDS[b] for b in context["params"]["branches"]]

    branch_a_finetune = SSHOperator(
        task_id="branch_a_finetune",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}-branch-a "
            "{{ params.timeout_branch_minutes }} {{ params.use_gpu | lower }} "
            "scripts/supervised_finetune_pass.py --lang {{ params.lang }} "
            "--label-method {{ params.label_method }}"
        ),
    )

    branch_b_finetune = SSHOperator(
        task_id="branch_b_finetune",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}-branch-b "
            "{{ params.timeout_branch_minutes }} {{ params.use_gpu | lower }} "
            "scripts/supervised_finetune_pass.py --lang {{ params.lang }} "
            "--label-method {{ params.label_method }} "
            "--base-model {{ params.base_model_b }} --out-tag b_mling"
        ),
    )

    branch_c_lora = SSHOperator(
        task_id="branch_c_lora",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}-branch-c "
            "{{ params.timeout_branch_minutes }} {{ params.use_gpu | lower }} "
            "scripts/embed_branch_c.py --lang {{ params.lang }} "
            "--label-method {{ params.label_method }} "
            "--embed-sample-size {{ params.embed_sample_size }}"
        ),
    )

    # Deliberately CPU-only regardless of use_gpu -- format normalization/extraction
    # and CBOW word vectors don't use the GPU.
    branch_cbow = SSHOperator(
        task_id="branch_cbow",
        ssh_conn_id=SSH_CONN_ID,
        cmd_timeout=None,
        command=(
            f"{WATCHDOG} {{{{ dag_run.run_id | replace(':', '-') | replace('+', '-') }}}}-branch-cbow "
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
