# =============================================================================
# ServiceNow IT Service Agent Dockerfile
# =============================================================================
# WHY: Containerizes the ServiceNow Agent for incident remediation
# HOW: Python 3.11 + LangGraph + Swarm RAG + Kafka streaming
# =============================================================================

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including build tools for hdbscan
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    gcc \
    g++ \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY agents/servicenow_agent/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy ServiceNow agent source code
COPY agents/servicenow_agent/ ./agents/servicenow_agent/

# Copy shared platform services
COPY platform_services/ ./platform_services/

# Copy MCP servers (used by ServiceNow agent for tool invocation)
COPY mcp-servers/ ./mcp-servers/

# Copy embeddings data
COPY data/ ./data/

# Environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose port
EXPOSE 8000

# Default command - run the orchestrator API
CMD ["python", "-m", "uvicorn", "agents.servicenow_agent.src.api.control_plane:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
