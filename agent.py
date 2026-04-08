import os
import logging
import datetime

import google.cloud.logging
import google.auth
from google.auth.transport.requests import Request
from google.cloud import datastore
from dotenv import load_dotenv

from mcp.server.fastmcp import FastMCP
from google.adk import Agent

from google.adk.tools.langchain_tool import LangchainTool
from langchain_community.tools import WikipediaQueryRun
from langchain_community.utilities import WikipediaAPIWrapper

from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams


try:
    google.cloud.logging.Client().setup_logging()
except Exception:
    logging.basicConfig(level=logging.INFO)

_ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=_ENV_PATH)

MODEL = os.getenv("MODEL", "gemini-2.5-flash-lite")
DB_ID = os.getenv("DB_ID", "genasdb")
PROJECT_ID = os.getenv("PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCLOUD_PROJECT") or "project_not_set"
DATASET_NAME = os.getenv("BQ_DATASET", "marketdata")
BIGQUERY_MCP_URL = os.getenv("BIGQUERY_MCP_URL", "https://bigquery.googleapis.com/mcp")

logging.info(f"ENV_PATH={_ENV_PATH}")
logging.info(f"Using MODEL={MODEL}, DB_ID={DB_ID}, PROJECT_ID={PROJECT_ID}, DATASET={DATASET_NAME}, BQ_MCP_URL={BIGQUERY_MCP_URL}")

db = datastore.Client(database=DB_ID)
mcp = FastMCP("WorkspaceTools")

_bq_toolset = None
_bq_init_error = None


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def _ensure_bigquery_toolset():
    global _bq_toolset, _bq_init_error
    if _bq_toolset is not None:
        return _bq_toolset
    if _bq_init_error is not None:
        return None

    try:
        credentials, project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
        credentials.refresh(Request())
        headers = {
            "Authorization": f"Bearer {credentials.token}",
            "x-goog-user-project": project_id,
        }
        _bq_toolset = MCPToolset(
            connection_params=StreamableHTTPConnectionParams(
                url=BIGQUERY_MCP_URL,
                headers=headers,
                timeout=30.0,
                sse_read_timeout=300.0,
            )
        )
        return _bq_toolset
    except Exception as e:
        logging.exception("Failed to init BigQuery MCP toolset")
        _bq_init_error = str(e)
        return None


def _list_bq_tools():
    ts = _ensure_bigquery_toolset()
    if ts is None:
        return []
    tools_attr = getattr(ts, "tools", None)
    if isinstance(tools_attr, dict):
        return list(tools_attr.keys())
    if isinstance(tools_attr, list):
        return [getattr(t, "name", str(t)) for t in tools_attr]
    list_tools = getattr(ts, "list_tools", None)
    if callable(list_tools):
        try:
            lst = list_tools()
            return [getattr(t, "name", str(t)) for t in lst]
        except Exception:
            return []
    return []


def _call_execute_sql_readonly(project_id: str, query: str):
    ts = _ensure_bigquery_toolset()
    if ts is None:
        raise RuntimeError(f"BigQuery MCP toolset init failed: {_bq_init_error}")

    get_tool = getattr(ts, "get_tool", None)
    if callable(get_tool):
        tool = get_tool("execute_sql_readonly")
        return tool(projectId=project_id, query=query)

    tools_attr = getattr(ts, "tools", None)
    if isinstance(tools_attr, dict) and "execute_sql_readonly" in tools_attr:
        tool = tools_attr["execute_sql_readonly"]
        return tool(projectId=project_id, query=query)

    if isinstance(tools_attr, list):
        for t in tools_attr:
            if getattr(t, "name", None) == "execute_sql_readonly":
                return t(projectId=project_id, query=query)

    call_tool = getattr(ts, "call_tool", None)
    if callable(call_tool):
        return call_tool("execute_sql_readonly", {"projectId": project_id, "query": query})

    raise RuntimeError(f"Cannot find execute_sql_readonly. Available tools: {_list_bq_tools()}")


@mcp.tool()
def add_task(title: str) -> str:
    try:
        key = db.key("Task")
        task = datastore.Entity(key=key)
        task.update({"title": title, "completed": False, "created_at": _now()})
        db.put(task)
        return f"✅ Task created: '{title}' (ID: {task.key.id})"
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
        lines = ["📋 Current Tasks:"]
        for t in tasks:
            status = "✅" if t.get("completed") else "⏳"
            lines.append(f"{status} {t.get('title')} (ID: {t.key.id})")
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
            return f"Task {numeric_id} not found."
        task["completed"] = True
        task["completed_at"] = _now()
        db.put(task)
        return f"✅ Task {numeric_id} marked as done."
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
        return f"📝 Note '{title}' saved successfully."
    except Exception as e:
        logging.exception("DB Error in add_note")
        return f"Database Error: {e}"


@mcp.tool()
def list_notes(limit: int = 20) -> str:
    try:
        q = db.query(kind="Note")
        q.order = ["-created_at"]
        notes = list(q.fetch(limit=max(1, min(limit, 50))))
        if not notes:
            return "No notes yet."
        lines = ["🗂️ Recent Notes:"]
        for n in notes:
            lines.append(f"• {n.get('title')} (ID: {n.key.id})")
        return "\n".join(lines)
    except Exception as e:
        logging.exception("DB Error in list_notes")
        return f"Database Error: {e}"


@mcp.tool()
def bq_sql(query: str) -> str:
    q = (query or "").strip()
    if not q:
        return "Empty SQL query."
    try:
        result = _call_execute_sql_readonly(PROJECT_ID, q)
        return str(result)
    except Exception as e:
        logging.exception("BigQuery query failed")
        return f"BigQuery Error: {e}\nAvailable tools: {_list_bq_tools()}\nInit error: {_bq_init_error}"


@mcp.tool()
def bq_diag() -> str:
    ts = _ensure_bigquery_toolset()
    return (
        "BigQuery Diagnostics\n"
        f"- PROJECT_ID={PROJECT_ID}\n"
        f"- DATASET={DATASET_NAME}\n"
        f"- BIGQUERY_MCP_URL={BIGQUERY_MCP_URL}\n"
        f"- toolset_initialized={ts is not None}\n"
        f"- init_error={_bq_init_error}\n"
        f"- available_tools={_list_bq_tools()}\n"
    )


wikipedia_tool = LangchainTool(tool=WikipediaQueryRun(api_wrapper=WikipediaAPIWrapper()))

INSTRUCTION = f"""
You are a Personal Workspace Assistant.

Capabilities:
- Tasks: add tasks, list tasks, complete tasks
- Notes: save notes, list notes
- Research: Wikipedia lookup + summary
- Dataset analytics: BigQuery (read-only SQL)

Tool rules:
- For BigQuery, ALWAYS call bq_sql with the SQL string.
- Never call any other BigQuery tool directly.
- If bq_sql returns an error, show the error message as-is.

BigQuery constraints:
- Use ONLY dataset `{DATASET_NAME}` in project `{PROJECT_ID}`.
- Allowed tables:
  - `{PROJECT_ID}.{DATASET_NAME}.gold_silver_raw`
  - `{PROJECT_ID}.{DATASET_NAME}.crypto_top1000_raw`
  - `{PROJECT_ID}.{DATASET_NAME}.company_financials_raw`
"""

tools_list = [
    add_task,
    list_tasks,
    complete_task,
    add_note,
    list_notes,
    wikipedia_tool,
    bq_sql,
    bq_diag,
]

root_agent = Agent(
    name="root_agent",
    model=MODEL,
    instruction=INSTRUCTION,
    tools=tools_list,
)