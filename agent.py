import os
import logging
import datetime
import google.cloud.logging

from google.cloud import datastore
from dotenv import load_dotenv

from mcp.server.fastmcp import FastMCP
from google.adk import Agent

from google.adk.tools.langchain_tool import LangchainTool
from langchain_community.tools import WikipediaQueryRun
from langchain_community.utilities import WikipediaAPIWrapper

from . import tools


try:
    google.cloud.logging.Client().setup_logging()
except Exception:
    logging.basicConfig(level=logging.INFO)

load_dotenv()

MODEL = os.getenv("MODEL", "gemini-2.5-flash-lite")
DB_ID = os.getenv("DB_ID", "genasdb")

PROJECT_ID = os.getenv("PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or "project_not_set"
BQ_DATASET = os.getenv("BQ_DATASET", "marketdata")

logging.info(f"MODEL={MODEL} DB_ID={DB_ID} PROJECT_ID={PROJECT_ID} BQ_DATASET={BQ_DATASET}")

db = datastore.Client(database=DB_ID)
mcp = FastMCP("WorkspaceTools")


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


@mcp.tool()
def add_task(title: str) -> str:
    try:
        key = db.key("Task")
        task = datastore.Entity(key=key)
        task.update({"title": title, "completed": False, "created_at": _now()})
        db.put(task)
        return f"Task created: '{title}' (ID: {task.key.id})"
    except Exception as e:
        logging.exception("DB Error in add_task")
        return f"Database Error: {e}"


@mcp.tool()
def list_tasks() -> str:
    try:
        q = db.query(kind="Task")
        q.order = ["-created_at"]
        tasks = list(q.fetch(limit=50))
        if not tasks:
            return "Your task list is empty."

        lines = ["Current Tasks:"]
        for t in tasks:
            status = "Done" if t.get("completed") else "Todo"
            lines.append(f"- [{status}] {t.get('title')} (ID: {t.key.id})")
        return "\n".join(lines)
    except Exception as e:
        logging.exception("DB Error in list_tasks")
        return f"Database Error: {e}"


@mcp.tool()
def complete_task(task_id: str) -> str:
    try:
        numeric_id = int("".join(filter(str.isdigit, task_id)))
        key = db.key("Task", numeric_id)
        task = db.get(key)
        if not task:
            return f"Task {numeric_id} not found. Run 'List my tasks' and copy the ID."
        task["completed"] = True
        task["completed_at"] = _now()
        db.put(task)
        return f"Task {numeric_id} marked as done."
    except Exception as e:
        logging.exception("Error in complete_task")
        return f"Error: {e}"


@mcp.tool()
def add_note(title: str, content: str) -> str:
    try:
        key = db.key("Note")
        note = datastore.Entity(key=key)
        note.update({"title": title, "content": content, "created_at": _now()})
        db.put(note)
        return f"Note '{title}' saved."
    except Exception as e:
        logging.exception("DB Error in add_note")
        return f"Database Error: {e}"


@mcp.tool()
def list_notes(limit: int = 20) -> str:
    try:
        q = db.query(kind="Note")
        q.order = ["-created_at"]
        notes = list(q.fetch(limit=max(1, min(int(limit), 50))))
        if not notes:
            return "No notes yet."

        lines = ["Recent Notes:"]
        for n in notes:
            lines.append(f"- {n.get('title')} (ID: {n.key.id})")
        return "\n".join(lines)
    except Exception as e:
        logging.exception("DB Error in list_notes")
        return f"Database Error: {e}"


@mcp.tool()
def bq_list_tables() -> str:
    return tools.list_tables()


@mcp.tool()
def bq_preview(table_id: str, limit: int = 5) -> str:
    return tools.preview_table(table_id=table_id, limit=limit)


@mcp.tool()
def bq_sql(sql: str, max_rows: int = 50) -> str:
    return tools.sql_query(sql=sql, max_rows=max_rows)


wikipedia_tool = LangchainTool(tool=WikipediaQueryRun(api_wrapper=WikipediaAPIWrapper()))


INSTRUCTION = f"""
You are a Personal Workspace Assistant.

You can:
- Manage tasks (add_task, list_tasks, complete_task)
- Manage notes (add_note, list_notes)
- Use Wikipedia for quick factual summaries
- Use BigQuery for dataset analysis via:
  - bq_list_tables
  - bq_preview
  - bq_sql

BigQuery constraints:
- Project: {PROJECT_ID}
- Dataset: {BQ_DATASET}
- Allowed tables:
  - {BQ_DATASET}.gold_silver_raw
  - {BQ_DATASET}.crypto_top1000_raw
  - {BQ_DATASET}.company_financials_raw

When the user asks for BigQuery analytics:
1) If you are unsure about columns, call bq_preview(table_id, 5) first.
2) Then call bq_sql with a SELECT/WITH query only.

Do not output tool calls as code. Call tools directly.
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
        bq_list_tables,
        bq_preview,
        bq_sql,
    ],
)