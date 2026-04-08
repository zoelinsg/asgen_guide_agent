import dotenv
import google.auth
from google.auth.transport.requests import Request
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams

BIGQUERY_MCP_URL = "https://bigquery.googleapis.com/mcp"


def get_bigquery_mcp_toolset() -> MCPToolset:
    """
    Google-hosted BigQuery MCP toolset.
    Uses Application Default Credentials (ADC) on Cloud Run / Cloud Shell.
    """
    dotenv.load_dotenv()

    credentials, project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )

    credentials.refresh(Request())
    oauth_token = credentials.token

    headers = {
        "Authorization": f"Bearer {oauth_token}",
        "x-goog-user-project": project_id,  # quota/billing attribution
    }

    return MCPToolset(
        connection_params=StreamableHTTPConnectionParams(
            url=BIGQUERY_MCP_URL,
            headers=headers,
            timeout=30.0,
            sse_read_timeout=300.0,
        )
    )