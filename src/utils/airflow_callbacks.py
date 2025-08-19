import os
import json
import requests


def _post_slack(webhook_url: str, text: str, blocks=None):
    payload = {"text": text}
    if blocks is not None:
        payload["blocks"] = blocks
    try:
        resp = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=10)
        resp.raise_for_status()
    except Exception:
        # Do not raise within Airflow callback
        pass


def task_failure_alert(context):
    """Airflow task failure callback sending Slack alert if webhook is set.

    Expects SLACK_WEBHOOK_URL env var or Airflow Connection 'slack_default' with password.
    """
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        # Try Airflow Connection slack_default
        try:
            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection("slack_default")
            webhook = conn.password or (conn.extra_dejson.get("webhook") if conn.extra else None)
        except Exception:
            webhook = None

    if not webhook:
        return  # silently skip

    ti = context.get("task_instance")
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else context.get("dag_id")
    task_id = ti.task_id if ti else context.get("task_id")
    run_id = context.get("run_id")
    error = context.get("exception")
    execution_date = context.get("ds") or context.get("execution_date")

    text = f":rotating_light: Task failed\nDAG: {dag_id}\nTask: {task_id}\nRun: {run_id}\nWhen: {execution_date}\nError: {error}"
    _post_slack(webhook, text)
