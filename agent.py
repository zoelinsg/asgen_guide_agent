import os
import re
import json
import logging
import datetime
import google.cloud.logging

from dotenv import load_dotenv
from google.cloud import datastore
from google.cloud import bigquery

from mcp.server.fastmcp import FastMCP
from google.adk import Agent

from google.adk.tools.langchain_tool import LangchainTool
from langchain_community.tools import WikipediaQueryRun
from langchain_community.utilities import WikipediaAPIWrapper


try:
    google.cloud.logging.Client().setup_logging()
except Exception:
    logging.basicConfig(level=logging.INFO)

load_dotenv()

MODEL = os.getenv("MODEL", "gemini-2.5-flash-lite")
DB_ID = os.getenv("DB_ID", "genasdb")
PROJECT_ID = os.getenv("PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or ""
BQ_DATASET = os.getenv("BQ_DATASET", "marketdata")

if not PROJECT_ID:
    PROJECT_ID = "project_not_set"

logging.info(f"MODEL={MODEL} DB_ID={DB_ID} PROJECT_ID={PROJECT_ID} BQ_DATASET={BQ_DATASET}")

db = datastore.Client(database=DB_ID)
bq = bigquery.Client(project=PROJECT_ID)

mcp = FastMCP("WorkspaceTools")


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def _is_safe_readonly_sql(sql: str) -> bool:
    s = sql.strip().lower()
    if not s.startswith("select") and not s.startswith("with"):
        return False
    forbidden = ["insert", "update", "delete", "merge", "drop", "alter", "create", "grant", "revoke"]
    return not any(word in s for word in forbidden)


def _render_rows(rows, max_rows: int = 20) -> str:
    rows = list(rows)
    if not rows:
        return "No rows returned."
    rows = rows[:max_rows]
    cols = list(rows[0].keys())
    lines = []
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for r in rows:
        vals = []
        for c in cols:
            v = r.get(c)
            if isinstance(v, (dict, list)):
                v = json.dumps(v, ensure_ascii=False)
            vals.append(str(v))
        lines.append("| " + " | ".join(vals) + " |")
    if len(rows) == max_rows:
        lines.append(f"\n(Showing first {max_rows} rows.)")
    return "\n".join(lines)


@mcp.tool()
def add_task(title: str) -> str:
    try:
        key = db.key("Task")
        task = datastore.Entity(key=key)
        task.update({"title": title, "completed": False, "created_at": _now()})
        db.put(task)
        return f"Task created: '{title}' (ID: {task.key.id})"
    except Exception as e:
        logging.exception("add_task failed")
        return f"Database Error: {e}"


@mcp.tool()
def list_tasks(limit: int = 50) -> str:
    try:
        q = db.query(kind="Task")
        q.order = ["-created_at"]
        tasks = list(q.fetch(limit=max(1, min(limit, 100))))
        if not tasks:
            return "Your task list is empty."
        lines = ["Here are your current tasks:"]
        for t in tasks:
            status = "done" if t.get("completed") else "todo"
            lines.append(f"- [{status}] {t.get('title')} (ID: {t.key.id})")
        return "\n".join(lines)
    except Exception as e:
        logging.exception("list_tasks failed")
        return f"Database Error: {e}"


@mcp.tool()
def complete_task(task_id: str) -> str:
    try:
        numeric_id = int("".join(filter(str.isdigit, task_id)))
        key = db.key("Task", numeric_id)
        task = db.get(key)
        if not task:
            return f"Task {numeric_id} not found."
        task["completed"] = True
        task["completed_at"] = _now()
        db.put(task)
        return f"Task {numeric_id} marked as done."
    except Exception as e:
        logging.exception("complete_task failed")
        return f"Error: {e}"


@mcp.tool()
def add_note(title: str, content: str) -> str:
    try:
        key = db.key("Note")
        note = datastore.Entity(key=key)
        note.update({"title": title, "content": content, "created_at": _now()})
        db.put(note)
        return f"Note '{title}' saved successfully."
    except Exception as e:
        logging.exception("add_note failed")
        return f"Database Error: {e}"


@mcp.tool()
def list_notes(limit: int = 20) -> str:
    try:
        q = db.query(kind="Note")
        q.order = ["-created_at"]
        notes = list(q.fetch(limit=max(1, min(limit, 50))))
        if not notes:
            return "No notes yet."
        lines = ["Recent notes:"]
        for n in notes:
            lines.append(f"- {n.get('title')} (ID: {n.key.id})")
        return "\n".join(lines)
    except Exception as e:
        logging.exception("list_notes failed")
        return f"Database Error: {e}"


@mcp.tool()
def bq_diag() -> str:
    try:
        ds = f"{PROJECT_ID}.{BQ_DATASET}"
        tables = list(bq.list_tables(ds))
        names = [t.table_id for t in tables]
        return f"BigQuery OK. Dataset={ds}. Tables={names}"
    except Exception as e:
        logging.exception("bq_diag failed")
        return f"BigQuery Error: {e}"


@mcp.tool()
def bq_sql(query: str) -> str:
    try:
        if not PROJECT_ID or PROJECT_ID == "project_not_set":
            return "BigQuery is not configured: PROJECT_ID is missing."
        if not _is_safe_readonly_sql(query):
            return "Only read-only SELECT/WITH queries are allowed."
        job = bq.query(query)
        rows = list(job.result())
        return _render_rows(rows, max_rows=20)
    except Exception as e:
        logging.exception("bq_sql failed")
        return f"BigQuery Error: {e}"


wikipedia_tool = LangchainTool(tool=WikipediaQueryRun(api_wrapper=WikipediaAPIWrapper()))

INSTRUCTION = f"""
You are a Personal Workspace Assistant.

You can:
- Manage tasks (add_task, list_tasks, complete_task)
- Manage notes (add_note, list_notes)
- Use Wikipedia (wikipedia) for quick facts
- Query BigQuery (bq_sql) for dataset analytics

Tool usage rules:
- If a tool is needed, call the tool directly by name.
- Never write tool calls as code. No "print()", no "default_api", no pseudo-Python.
- After a tool returns, summarize the result briefly in natural language.

BigQuery rules:
- Dataset: `{PROJECT_ID}.{BQ_DATASET}`
- Allowed tables:
  - `{PROJECT_ID}.{BQ_DATASET}.gold_silver_raw`
  - `{PROJECT_ID}.{BQ_DATASET}.crypto_top1000_raw`
  - `{PROJECT_ID}.{BQ_DATASET}.company_financials_raw`
- If unsure about columns, first do:
  SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.table_name` LIMIT 5
"""

root_agent = Agent(
    name="root_agent",
    model=MODEL,
    instruction=INSTRUCTION,
    tools=[
        add_task,
        list_tasks,
        complete_task,
        add_note,
        list_notes,
        wikipedia_tool,
        bq_diag,
        bq_sql,
    ],
)