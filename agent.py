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


# ----------------------------
# Logging & Env
# ----------------------------
try:
    google.cloud.logging.Client().setup_logging()
except Exception:
    logging.basicConfig(level=logging.INFO)

load_dotenv()

MODEL = os.getenv("MODEL", "gemini-2.5-flash-lite")
DB_ID = os.getenv("DB_ID", "genasdb")
PROJECT_ID = os.getenv("PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or "project_not_set"
DATASET_NAME = os.getenv("BQ_DATASET", "marketdata")

logging.info(f"Using MODEL={MODEL}, DB_ID={DB_ID}, PROJECT_ID={PROJECT_ID}, DATASET={DATASET_NAME}")

BIGQUERY_MCP_URL = os.getenv("BIGQUERY_MCP_URL", "https://bigquery.googleapis.com/mcp")


# ----------------------------
# BigQuery MCP Toolset
# ----------------------------
def get_bigquery_mcp_toolset() -> MCPToolset:
    credentials, project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    # Refresh token to ensure bearer is present
    credentials.refresh(Request())

    headers = {
        "Authorization": f"Bearer {credentials.token}",
        "x-goog-user-project": project_id,
    }

    return MCPToolset(
        connection_params=StreamableHTTPConnectionParams(
            url=BIGQUERY_MCP_URL,
            headers=headers,
            timeout=30.0,
            sse_read_timeout=300.0,
        )
    )


try:
    bigquery_toolset = get_bigquery_mcp_toolset()
except Exception:
    logging.exception("Failed to init BigQuery toolset")
    bigquery_toolset = None


def _list_bq_tools() -> list[str]:
    """Return tool names exposed by MCPToolset (best-effort across versions)."""
    if bigquery_toolset is None:
        return []

    tools_attr = getattr(bigquery_toolset, "tools", None)
    if isinstance(tools_attr, dict):
        return list(tools_attr.keys())

    if isinstance(tools_attr, list):
        return [getattr(t, "name", str(t)) for t in tools_attr]

    list_tools = getattr(bigquery_toolset, "list_tools", None)
    if callable(list_tools):
        try:
            ts = list_tools()
            return [getattr(t, "name", str(t)) for t in ts]
        except Exception:
            return []

    return []


def _call_execute_sql_readonly(project_id: str, query: str):
    """
    Call BigQuery MCP execute_sql_readonly using multiple access patterns to handle version differences.
    """
    if bigquery_toolset is None:
        raise RuntimeError("BigQuery toolset is not initialized.")

    # Pattern A: get_tool("execute_sql_readonly")
    get_tool = getattr(bigquery_toolset, "get_tool", None)
    if callable(get_tool):
        tool = get_tool("execute_sql_readonly")
        return tool(projectId=project_id, query=query)

    # Pattern B: tools dict-like (bigquery_toolset.tools)
    tools_attr = getattr(bigquery_toolset, "tools", None)
    if isinstance(tools_attr, dict) and "execute_sql_readonly" in tools_attr:
        tool = tools_attr["execute_sql_readonly"]
        return tool(projectId=project_id, query=query)

    # Pattern C: list of tools with name attribute
    if isinstance(tools_attr, list):
        for t in tools_attr:
            if getattr(t, "name", None) == "execute_sql_readonly":
                return t(projectId=project_id, query=query)

    # Pattern D: call_tool("execute_sql_readonly", args)
    call_tool = getattr(bigquery_toolset, "call_tool", None)
    if callable(call_tool):
        return call_tool("execute_sql_readonly", {"projectId": project_id, "query": query})

    raise RuntimeError(f"Cannot call execute_sql_readonly. Available tools: {_list_bq_tools()}")


# ----------------------------
# Datastore + MCP local tools (Tasks / Notes)
# ----------------------------
db = datastore.Client(database=DB_ID)
mcp = FastMCP("WorkspaceTools")


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


@mcp.tool()
def add_task(title: str) -> str:
    """Add a new task."""
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
    """List all tasks (latest first)."""
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
    """Mark a task as completed. Use the numeric ID shown in list_tasks()."""
    try:
        numeric_id = int("".join(filter(str.isdigit, task_id)))
        key = db.key("Task", numeric_id)
        task = db.get(key)
        if not task:
            return f"Task {numeric_id} not found. Tip: run 'List my tasks' and copy the ID."
        task["completed"] = True
        task["completed_at"] = _now()
        db.put(task)
        return f"✅ Task {numeric_id} marked as done."
    except Exception as e:
        logging.exception("Error in complete_task")
        return f"Error: {e}"


@mcp.tool()
def add_note(title: str, content: str) -> str:
    """Save a note."""
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
    """List recent notes (titles only)."""
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


# ----------------------------
# BigQuery wrapper tool (the ONLY BigQuery tool the model should call)
# ----------------------------
@mcp.tool()
def bq_sql(query: str) -> str:
    """
    Run a read-only BigQuery SQL query via MCP and return results as text.
    If it fails, it will return the error plus available tool names for debugging.
    """
    if bigquery_toolset is None:
        return "BigQuery tool is not available (toolset init failed)."

    q = (query or "").strip()
    if not q:
        return "Empty SQL query."

    try:
        result = _call_execute_sql_readonly(PROJECT_ID, q)
        return str(result)
    except Exception as e:
        logging.exception("BigQuery query failed")
        return f"BigQuery Error: {e}\nAvailable tools: {_list_bq_tools()}"


# ----------------------------
# Wikipedia tool
# ----------------------------
wikipedia_tool = LangchainTool(tool=WikipediaQueryRun(api_wrapper=WikipediaAPIWrapper()))


# ----------------------------
# Root Agent Instruction
# ----------------------------
INSTRUCTION = f"""
You are a Personal Workspace Assistant.

Capabilities:
- Tasks: add tasks, list tasks, complete tasks (persistent via Datastore)
- Notes: save notes, list notes (persistent via Datastore)
- Research: Wikipedia lookup + summary
- Dataset analytics: BigQuery (read-only SQL)

STRICT Tool Calling Rules:
- If a tool is needed, ALWAYS call the tool directly (never wrap tool calls in text).
- NEVER use "print()", NEVER use "default_api", NEVER output "call ..." as text.
- NEVER write code-like tool calls. Tool calls must be structured tool calls only.
- After tool execution, summarize results in natural language.

BigQuery Rules (IMPORTANT):
- For BigQuery, ALWAYS call the tool "bq_sql" with the SQL string.
- Never call any other BigQuery tool directly.
- Query ONLY project "{PROJECT_ID}" and dataset "{DATASET_NAME}".
- Allowed tables:
  - `{PROJECT_ID}.{DATASET_NAME}.gold_silver_raw`
  - `{PROJECT_ID}.{DATASET_NAME}.crypto_top1000_raw`
  - `{PROJECT_ID}.{DATASET_NAME}.company_financials_raw`
- Do NOT invent columns. If unsure, first run:
  SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.TABLE_NAME` LIMIT 5
- If bq_sql returns an error, show the error message as-is without rewriting it.

SQL Formatting Rules:
- Use Standard SQL.
- Use fully-qualified table names with backticks: `{PROJECT_ID}.{DATASET_NAME}.table`
- Keep queries small. Prefer LIMIT 10 for previews.

Recommended BigQuery Queries:
- Latest gold/silver date:
  SELECT MAX(date) AS latest_date
  FROM `{PROJECT_ID}.{DATASET_NAME}.gold_silver_raw`;

- Top 10 crypto by marketCap:
  SELECT name, symbol, marketCap, price, `24hVolume`, change, listedAt
  FROM `{PROJECT_ID}.{DATASET_NAME}.crypto_top1000_raw`
  ORDER BY marketCap DESC
  LIMIT 10;
"""


# ----------------------------
# Tools list + Agent
# ----------------------------
tools_list = [
    add_task,
    list_tasks,
    complete_task,
    add_note,
    list_notes,
    wikipedia_tool,
    bq_sql,
]

root_agent = Agent(
    name="root_agent",
    model=MODEL,
    instruction=INSTRUCTION,
    tools=tools_list,
)