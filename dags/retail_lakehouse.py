from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

REPO = Path("/opt/retail")

@dag(
    dag_id="retail_lakehouse",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["retail", "delta", "dbt"],
)
def retail_lakehouse():
    @task
    def preflight() -> None:
        from pipelines.config import Settings
        from pipelines.source import record_source_check
        settings = Settings.from_env()
        record_source_check(settings, "SUCCESS", {"component":"airflow-preflight"})

    @task
    def claim_batch() -> dict:
        from pipelines.config import Settings
        from pipelines.source import claim_cdc_batch
        settings = Settings.from_env()
        if settings.input_mode != "cdc":
            raise AirflowFailException("S3 file mode is executed by the dedicated manifest workflow until its snapshot converter is selected")
        return claim_cdc_batch(settings)

    @task
    def bronze(batch: dict) -> dict:
        if not batch["batch_id"]:
            return batch
        completed = subprocess.run(["python", "-m", "pipelines.bronze", batch["batch_id"]], cwd=REPO, text=True, capture_output=True)
        if completed.returncode:
            raise AirflowFailException(completed.stderr[-2000:])
        return json.loads(completed.stdout.strip().splitlines()[-1])

    @task
    def dbt_build(result: dict) -> dict:
        if not result.get("batch_id"):
            return result
        completed = subprocess.run(["/opt/dbt-venv/bin/dbt", "build", "--project-dir", str(REPO / "airflow_dbt_project" / "walmart_project"), "--profiles-dir", str(REPO / "airflow_dbt_project" / "walmart_project"), "--target-path", "/opt/lakehouse/dbt-target", "--log-path", "/opt/lakehouse/dbt-logs"], cwd=REPO, text=True, capture_output=True)
        if completed.returncode:
            raise AirflowFailException(completed.stdout[-2000:] + completed.stderr[-2000:])
        from pipelines.config import Settings
        from pipelines.source import set_batch_state
        set_batch_state(Settings.from_env(), result["batch_id"], "TRANSFORMED", {"dbt":"build passed"})
        return result

    @task
    def publish(result: dict) -> None:
        if not result.get("batch_id"):
            return
        from pipelines.config import Settings
        from pipelines.source import set_batch_state
        set_batch_state(Settings.from_env(), result["batch_id"], "PUBLISHED", {"published_at":datetime.utcnow().isoformat()})

    check = preflight()
    batch = claim_batch()
    check >> batch
    publish(dbt_build(bronze(batch)))

retail_lakehouse()
