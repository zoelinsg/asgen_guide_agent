import os
import re
import logging
from typing import Optional

from google.cloud import bigquery


def _get_project_id() -> str:
    return (
        os.getenv("PROJECT_ID")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCP_PROJECT")
        or ""
    ).strip()


def _get_dataset() -> str:
    return (os.getenv("BQ_DATASET") or "marketdata").strip()


def _get_location() -> Optional[str]:
    loc = (os.getenv("BQ_LOCATION") or "").strip()
    return loc or None


def _client() -> bigquery.Client:
    project_id = _get_project_id() or None
    return bigquery.Client(project=project_id, location=_get_location())


def _is_readonly_sql(sql: str) -> bool:
    s = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE).strip().lower()
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL).strip().lower()
    if not s:
        return False
    if ";" in s:
        parts = [p.strip() for p in s.split(";") if p.strip()]
        if len(parts) != 1:
            return False
        s = parts[0]
    return s.startswith("select") or s.startswith("with")


def _enforce_dataset_scope(sql: str, project_id: str, dataset: str) -> None:
    allowed_prefix = f"`{project_id}.{dataset}."
    if allowed_prefix not in sql:
        raise ValueError(
            f"SQL must reference tables using fully-qualified backticks like {allowed_prefix}table_name`"
        )


def bq_diag() -> str:
    project_id = _get_project_id()
    dataset = _get_dataset()
    location = _get_location() or "(default)"
    if not project_id:
        return "PROJECT_ID is missing. Set PROJECT_ID (or GOOGLE_CLOUD_PROJECT) in .env."

    try:
        client = _client()
        ds_ref = bigquery.DatasetReference(project_id, dataset)
        client.get_dataset(ds_ref)
        return (
            "BigQuery diagnostics OK.\n"
            f"- PROJECT_ID: {project_id}\n"
            f"- DATASET: {dataset}\n"
            f"- LOCATION: {location}\n"
            "Dataset is accessible."
        )
    except Exception as e:
        logging.exception("bq_diag failed")
        return (
            "BigQuery diagnostics FAILED.\n"
            f"- PROJECT_ID: {project_id}\n"
            f"- DATASET: {dataset}\n"
            f"- LOCATION: {location}\n"
            f"- ERROR: {e}"
        )


def bq_sql(query: str, max_rows: int = 20) -> str:
    project_id = _get_project_id()
    dataset = _get_dataset()

    if not project_id:
        return "PROJECT_ID is missing. Set PROJECT_ID (or GOOGLE_CLOUD_PROJECT) in .env."
    if not query or not query.strip():
        return "Empty query."

    q = query.strip()
    if not _is_readonly_sql(q):
        return "Only read-only SQL is allowed (SELECT / WITH)."

    try:
        _enforce_dataset_scope(q, project_id, dataset)
    except Exception as e:
        return f"Query blocked: {e}"

    try:
        client = _client()
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        job = client.query(q, job_config=job_config)
        rows = list(job.result(max_results=max(1, min(int(max_rows), 100))))
        if not rows:
            return "Query OK. No rows returned."

        headers = list(rows[0].keys())
        out_lines = []
        out_lines.append(" | ".join(headers))
        out_lines.append("-" * min(120, max(10, len(out_lines[0]))))
        for r in rows:
            out_lines.append(" | ".join(str(r.get(h, "")) for h in headers))
        return "\n".join(out_lines)

    except Exception as e:
        logging.exception("bq_sql failed")
        return f"BigQuery query failed: {e}"