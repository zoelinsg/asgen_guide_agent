import os
import logging
from typing import Optional

from google.cloud import bigquery


def _get_project_id() -> str:
    return (
        os.getenv("PROJECT_ID")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCP_PROJECT")
        or "project_not_set"
    )


def _get_dataset() -> str:
    return os.getenv("BQ_DATASET", "marketdata")


def _bq_client() -> bigquery.Client:
    return bigquery.Client(project=_get_project_id())


def _is_safe_select_sql(sql: str) -> bool:
    s = sql.strip().lower()
    if not s.startswith("select") and not s.startswith("with"):
        return False
    blocked = [
        " insert ",
        " update ",
        " delete ",
        " merge ",
        " drop ",
        " alter ",
        " create ",
        " truncate ",
        " grant ",
        " revoke ",
        " call ",
        " begin ",
        " commit ",
        " rollback ",
    ]
    s_pad = f" {s} "
    return not any(b in s_pad for b in blocked)


def list_tables() -> str:
    project = _get_project_id()
    dataset = _get_dataset()
    client = _bq_client()

    try:
        tables = list(client.list_tables(f"{project}.{dataset}"))
        if not tables:
            return f"No tables found in `{project}.{dataset}`."

        lines = [f"Tables in `{project}.{dataset}`:"]
        for t in tables:
            lines.append(f"- {t.table_id}")
        return "\n".join(lines)
    except Exception as e:
        logging.exception("BigQuery list_tables failed")
        return f"BigQuery error: {e}"


def preview_table(table_id: str, limit: int = 5) -> str:
    project = _get_project_id()
    dataset = _get_dataset()

    table_id = table_id.strip()
    if "." in table_id:
        full = f"`{table_id}`"
    else:
        full = f"`{project}.{dataset}.{table_id}`"

    limit = max(1, min(int(limit), 50))
    sql = f"SELECT * FROM {full} LIMIT {limit}"

    return sql_query(sql)


def sql_query(sql: str, max_rows: int = 50) -> str:
    project = _get_project_id()
    client = _bq_client()

    if not _is_safe_select_sql(sql):
        return "Only SELECT/WITH queries are allowed."

    max_rows = max(1, min(int(max_rows), 200))

    try:
        job = client.query(sql)
        rows = list(job.result(max_results=max_rows))

        if not rows:
            return "Query ran successfully, but returned 0 rows."

        cols = list(rows[0].keys())

        def fmt(v):
            if v is None:
                return "NULL"
            s = str(v)
            return s if len(s) <= 120 else s[:117] + "..."

        header = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        body_lines = []
        for r in rows[:max_rows]:
            body_lines.append("| " + " | ".join(fmt(r.get(c)) for c in cols) + " |")

        return "\n".join([header, sep] + body_lines)

    except Exception as e:
        logging.exception("BigQuery sql_query failed")
        return f"BigQuery error: {e}"