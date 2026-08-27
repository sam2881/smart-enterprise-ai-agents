"""
MCP Servers - SINGLE SOURCE OF TRUTH for All Agent Tools

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!!  THIS IS THE ONLY LOCATION FOR MCP SERVERS               !!
!!  Do NOT create MCP servers in agents/ or elsewhere       !!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

WHY: Centralizes MCP tool servers that are shared by multiple agents:
- ServiceNow Agent: Uses RAG, GCS (logs), LLM for incident remediation
- Data Agent: Uses GCS (data lake), Iceberg, LLM for pipeline generation

HOW: Each server exposes MCP-compatible tools via FastAPI:
- /mcp/tools - List available tools
- /mcp/tools/call - Execute a tool

SERVERS:
- rag_server.py: RAG search operations (port 8010)
- gcs_server.py: GCS operations for both logs and data (port 8011)
- iceberg_server.py: Apache Iceberg catalog operations (port 8012)
- llm_server.py: LLM operations with determinism tracking (port 8013)
- airflow_server.py: Airflow DAG operations (port 8095)

USAGE:
    # Start all servers
    python -m backend.mcp.servers.start_all

    # Or start individual server
    python -m backend.mcp.servers.gcs_server
"""

try:
    from .rag_server import RAGMCPServer
except ImportError:
    RAGMCPServer = None
try:
    from .gcs_server import create_gcs_mcp_server
except ImportError:
    create_gcs_mcp_server = None
try:
    from .iceberg_server import create_iceberg_mcp_server
except ImportError:
    create_iceberg_mcp_server = None
try:
    from .llm_server import create_llm_mcp_server
except ImportError:
    create_llm_mcp_server = None
try:
    from .airflow_server import create_airflow_mcp_server
except ImportError:
    create_airflow_mcp_server = None

__all__ = [
    "RAGMCPServer",
    "create_gcs_mcp_server",
    "create_iceberg_mcp_server",
    "create_llm_mcp_server",
    "create_airflow_mcp_server",
]
