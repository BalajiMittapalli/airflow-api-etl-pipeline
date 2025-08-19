"""
Config-driven ETL DAG (TaskFlow API) orchestrating extract -> validate -> transform -> load.
Works for any YAML in configs/ by passing params.api_config (e.g., sample_api).
Schedule is read from the default config chosen at parse time via AIRFLOW_API_CONFIG env var.
"""

import os
import sys
import yaml
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

# Ensure src/ is importable inside Airflow container
AIRFLOW_ROOT = "/opt/airflow"
if AIRFLOW_ROOT not in sys.path:
    sys.path.append(AIRFLOW_ROOT)
SRC_PATH = os.path.join(AIRFLOW_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

from src.extractors.api_adapter import extract_api  # noqa: E402
from src.validators.schema_validator import validate_schema  # noqa: E402
from src.transformers.data_transformer import transform_data  # noqa: E402
from src.loaders.postgres_loader import load_to_postgres  # noqa: E402
from src.utils.airflow_callbacks import task_failure_alert  # noqa: E402


def _get_default_api_config() -> str:
    return os.environ.get("AIRFLOW_API_CONFIG", "sample_api")


def _get_schedule_from_config(api_config: str):
    cfg_path = os.path.join(AIRFLOW_ROOT, "configs", f"{api_config}.yaml")
    if not os.path.exists(cfg_path):
        return None
    try:
        with open(cfg_path, "r") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("schedule")
    except Exception:
        return None


default_api_config = _get_default_api_config()

dag_schedule = _get_schedule_from_config(default_api_config)

with DAG(
    dag_id="api_etl_dag",
    description="Config-driven ETL for any REST API via YAML",
    start_date=days_ago(1),
    schedule=dag_schedule,  # taken from default config at parse time
    catchup=False,
    params={
        "api_config": default_api_config,  # can be overridden on manual trigger
    },
    on_failure_callback=task_failure_alert,
    max_active_runs=1,
) as dag:

    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )
    def extract(**context):
        api_cfg = context["params"].get("api_config", default_api_config)
        execution_date = context.get("ds")  # YYYY-MM-DD
        config_path = os.path.join(AIRFLOW_ROOT, "configs", f"{api_cfg}.yaml")
        saved_files = extract_api(config_path, execution_date)
        # Return a small summary to XCom
        return {"files": len(saved_files)}

    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    def validate(**context):
        api_cfg = context["params"].get("api_config", default_api_config)
        execution_date = context.get("ds")
        config_path = os.path.join(AIRFLOW_ROOT, "configs", f"{api_cfg}.yaml")
        report = validate_schema(config_path, execution_date)
        # Fail fast if any validation errors reported, except benign "No data found."
        errors = report.get("errors", [])
        invalid = int(report.get("invalid_rows", 0) or 0)
        valid = int(report.get("valid_rows", 0) or 0)
        lower = lambda s: str(s).strip().lower()
        non_benign_errors = [
            e for e in errors
            if lower(e) != "no data found." and not lower(e).startswith("duplicate values found in unique_keys")
        ]
    if non_benign_errors or invalid > 0:
            raise AirflowException(
                f"Validation failed: invalid_rows={invalid}, errors_count={len(non_benign_errors)}"
            )
    # For no-data runs, just pass zeros and a note; downstream tasks can handle empty transforms
    note = "no data" if errors and not non_benign_errors else ""
    return {"valid_rows": valid, "invalid_rows": invalid, "note": note}

    @task()
    def transform(**context):
        api_cfg = context["params"].get("api_config", default_api_config)
        execution_date = context.get("ds")
        config_path = os.path.join(AIRFLOW_ROOT, "configs", f"{api_cfg}.yaml")
        df = transform_data(config_path, execution_date)
        # Avoid large XComs: return only counts
        row_count = 0 if df is None else int(getattr(df, "shape", [0])[0])
        return {"rows": row_count}

    @task()
    def load(**context):
        api_cfg = context["params"].get("api_config", default_api_config)
        execution_date = context.get("ds")
        config_path = os.path.join(AIRFLOW_ROOT, "configs", f"{api_cfg}.yaml")
        result = load_to_postgres(config_path, execution_date)
        return result

    # Task ordering
    extract() >> validate() >> transform() >> load()
