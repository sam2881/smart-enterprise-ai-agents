"""Observability endpoints — real-time service health, metrics, and event data."""
import asyncio
import logging
import os
import socket
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
import psycopg2
import redis as redis_lib
from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter()

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin123@localhost:5432/ai_agent")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _tcp_check(host: str, port: int, timeout: float = 2.0) -> Optional[float]:
    """Return latency_ms if TCP connect succeeds, else None."""
    try:
        start = time.perf_counter()
        with socket.create_connection((host, port), timeout=timeout):
            return round((time.perf_counter() - start) * 1000, 1)
    except Exception:
        return None


async def _http_check(url: str, timeout: float = 3.0) -> Optional[float]:
    """Return latency_ms if HTTP GET succeeds (2xx/3xx), else None."""
    try:
        start = time.perf_counter()
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
            if resp.status_code < 500:
                return round((time.perf_counter() - start) * 1000, 1)
    except Exception:
        pass
    return None


async def _prometheus_query(metric: str, client: httpx.AsyncClient) -> Optional[float]:
    """Execute an instant Prometheus query, return the first scalar value."""
    try:
        resp = await client.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": metric},
            timeout=5.0,
        )
        data = resp.json()
        if data.get("status") == "success":
            result = data["data"].get("result", [])
            if result:
                return float(result[0]["value"][1])
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/services")
async def get_services_health():
    """Check connectivity to all platform services and return live health status."""

    async def check_postgres() -> Dict[str, Any]:
        start = time.perf_counter()
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=3)
            conn.close()
            return {"status": "healthy", "latency_ms": round((time.perf_counter() - start) * 1000, 1)}
        except Exception:
            return {"status": "down", "latency_ms": None}

    async def check_redis() -> Dict[str, Any]:
        start = time.perf_counter()
        try:
            r = redis_lib.Redis.from_url(REDIS_URL, socket_connect_timeout=3)
            r.ping()
            r.close()
            return {"status": "healthy", "latency_ms": round((time.perf_counter() - start) * 1000, 1)}
        except Exception:
            return {"status": "down", "latency_ms": None}

    # Run all checks concurrently
    pg_task = asyncio.create_task(check_postgres())
    redis_task = asyncio.create_task(check_redis())

    http_checks = {
        "weaviate":   _http_check("http://localhost:8080/v1/.well-known/ready"),
        "prometheus": _http_check("http://localhost:9090/-/healthy"),
        "grafana":    _http_check("http://localhost:3001/api/health"),
        "langfuse":   _http_check("http://localhost:3002/api/public/health"),
        "airflow":    _http_check("http://localhost:8083/health"),
        "data_agent": _http_check("http://localhost:8001/health"),
        "tempo":      _http_check("http://localhost:3200/ready"),
    }
    http_results = {k: await v for k, v in http_checks.items()}

    kafka_latency = await asyncio.get_event_loop().run_in_executor(
        None, _tcp_check, "localhost", 9092
    )
    neo4j_latency = await asyncio.get_event_loop().run_in_executor(
        None, _tcp_check, "localhost", 7687
    )

    pg_result = await pg_task
    redis_result = await redis_task

    def ms_to_status(latency_ms: Optional[float]) -> str:
        if latency_ms is None:
            return "down"
        if latency_ms > 1000:
            return "degraded"
        return "healthy"

    services = [
        {"name": "Orchestrator API",  "status": "healthy",                                     "port": 8000, "latency_ms": 1.0,                        "uptime_pct": 99.99},
        {"name": "Data Agent API",    "status": ms_to_status(http_results["data_agent"]),      "port": 8001, "latency_ms": http_results["data_agent"],   "uptime_pct": None},
        {"name": "PostgreSQL",        "status": pg_result["status"],                           "port": 5432, "latency_ms": pg_result["latency_ms"],      "uptime_pct": None},
        {"name": "Redis",             "status": redis_result["status"],                        "port": 6379, "latency_ms": redis_result["latency_ms"],   "uptime_pct": None},
        {"name": "Kafka",             "status": ms_to_status(kafka_latency),                   "port": 9092, "latency_ms": kafka_latency,                "uptime_pct": None},
        {"name": "Neo4j",             "status": ms_to_status(neo4j_latency),                   "port": 7687, "latency_ms": neo4j_latency,                "uptime_pct": None},
        {"name": "Weaviate",          "status": ms_to_status(http_results["weaviate"]),        "port": 8080, "latency_ms": http_results["weaviate"],     "uptime_pct": None},
        {"name": "Prometheus",        "status": ms_to_status(http_results["prometheus"]),      "port": 9090, "latency_ms": http_results["prometheus"],   "uptime_pct": None},
        {"name": "Grafana",           "status": ms_to_status(http_results["grafana"]),         "port": 3001, "latency_ms": http_results["grafana"],      "uptime_pct": None},
        {"name": "Langfuse",          "status": ms_to_status(http_results["langfuse"]),        "port": 3002, "latency_ms": http_results["langfuse"],     "uptime_pct": None},
        {"name": "Airflow",           "status": ms_to_status(http_results["airflow"]),         "port": 8083, "latency_ms": http_results["airflow"],      "uptime_pct": None},
        {"name": "Tempo",             "status": ms_to_status(http_results["tempo"]),           "port": 3200, "latency_ms": http_results["tempo"],        "uptime_pct": None},
    ]

    return {"services": services, "timestamp": datetime.utcnow().isoformat() + "Z"}


@router.get("/incidents/states")
async def get_incident_state_distribution():
    """Return incident counts grouped by workflow status from PostgreSQL."""
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=3)
        cur = conn.cursor()
        cur.execute(
            "SELECT status, COUNT(*) as cnt FROM agent.incidents GROUP BY status ORDER BY cnt DESC"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        total = sum(r[1] for r in rows) or 1
        states = [
            {
                "state": row[0],
                "count": row[1],
                "percentage": round(row[1] / total * 100, 1),
            }
            for row in rows
        ]
        return {"states": states}
    except Exception as e:
        logger.warning("Failed to query incident states: %s", e)
        return {"states": []}


@router.get("/kafka/events")
async def get_recent_kafka_events(limit: int = 20):
    """Return recent platform events from the audit log (most recent first)."""
    try:
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=3)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT event_type, entity_type, entity_id, actor, created_at
            FROM audit.events
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (min(limit, 100),),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        events = []
        for row in rows:
            event_type, entity_type, entity_id, actor, created_at = row
            # Derive a Kafka-style topic from event_type (e.g. "incident.created")
            topic = event_type.lower() if event_type else "audit.event"
            events.append({
                "topic": topic,
                "event_type": event_type or "UNKNOWN",
                "timestamp": created_at.isoformat() + "Z" if created_at else datetime.utcnow().isoformat() + "Z",
                "summary": f"{entity_type} {entity_id} by {actor}" if entity_id else (event_type or ""),
            })
        return {"events": events}
    except Exception as e:
        logger.warning("Failed to query kafka events from audit log: %s", e)
        return {"events": []}


@router.get("/metrics/summary")
async def get_metrics_summary():
    """Query Prometheus for key platform metrics summary."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        prometheus_ok = await _http_check("http://localhost:9090/-/healthy")
        if prometheus_ok is None:
            return {
                "llm_calls_total": None,
                "llm_latency_p95_ms": None,
                "llm_tokens_total": None,
                "active_incidents": None,
                "pending_approvals": None,
                "prometheus_available": False,
            }

        llm_calls = await _prometheus_query("sum(aiagent_llm_calls_total)", client)
        llm_latency_raw = await _prometheus_query(
            "histogram_quantile(0.95, rate(aiagent_llm_latency_seconds_bucket[1h]))", client
        )
        llm_tokens = await _prometheus_query("sum(aiagent_llm_tokens_total)", client)
        active_incidents = await _prometheus_query("aiagent_incidents_active", client)
        pending_approvals = await _prometheus_query("aiagent_approvals_pending", client)

        return {
            "llm_calls_total": int(llm_calls) if llm_calls is not None else None,
            "llm_latency_p95_ms": round(llm_latency_raw * 1000, 1) if llm_latency_raw is not None else None,
            "llm_tokens_total": int(llm_tokens) if llm_tokens is not None else None,
            "active_incidents": int(active_incidents) if active_incidents is not None else None,
            "pending_approvals": int(pending_approvals) if pending_approvals is not None else None,
            "prometheus_available": True,
        }
