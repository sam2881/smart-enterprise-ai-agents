"""
APEX Data Agent - Feed Repository

PostgreSQL CRUD for FeedGroupConfig and FeedConfig.
"""

import json
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger()


class FeedRepository:
    """PostgreSQL repository for feed configurations."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def _get_conn(self):
        import psycopg2
        return psycopg2.connect(self.connection_string)

    def get_feed_group(self, feed_group_id: str) -> Optional[Dict[str, Any]]:
        """Load feed group configuration from PostgreSQL."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT config FROM feed_groups WHERE feed_group_id = %s",
                (feed_group_id,),
            )
            row = cur.fetchone()
            if row:
                config = row[0]
                return json.loads(config) if isinstance(config, str) else config
            return None
        finally:
            conn.close()

    def upsert_feed_group(self, feed_group_id: str, config: Dict[str, Any]):
        """Insert or update feed group configuration."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO feed_groups (feed_group_id, dag_id, domain, config, updated_at)
                   VALUES (%s, %s, %s, %s, NOW())
                   ON CONFLICT (feed_group_id) DO UPDATE SET
                     config = EXCLUDED.config,
                     updated_at = NOW()""",
                (
                    feed_group_id,
                    config.get("dag_id", ""),
                    config.get("domain", ""),
                    json.dumps(config),
                ),
            )
            conn.commit()
            logger.info("feed_group_upserted", feed_group_id=feed_group_id)
        finally:
            conn.close()

    def upsert_feed(self, feed_id: str, config: Dict[str, Any]):
        """Insert or update individual feed configuration."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO feed_configs (feed_id, feed_name, domain, source_type, config, updated_at)
                   VALUES (%s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (feed_id) DO UPDATE SET
                     config = EXCLUDED.config,
                     updated_at = NOW()""",
                (
                    feed_id,
                    config.get("feed_name", ""),
                    config.get("domain", ""),
                    config.get("source_type", ""),
                    json.dumps(config),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_feeds_by_domain(self, domain: str) -> List[Dict[str, Any]]:
        """Get all feeds for a domain."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT feed_id, config FROM feed_configs WHERE domain = %s ORDER BY feed_name",
                (domain,),
            )
            return [
                {"feed_id": row[0], **(json.loads(row[1]) if isinstance(row[1], str) else row[1])}
                for row in cur.fetchall()
            ]
        finally:
            conn.close()

    def log_execution(
        self,
        feed_id: str,
        execution_id: str,
        status: str,
        metrics: Optional[Dict[str, Any]] = None,
    ):
        """Log execution record."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO feed_executions
                   (feed_id, execution_id, status, metrics, created_at)
                   VALUES (%s, %s, %s, %s, NOW())""",
                (feed_id, execution_id, status, json.dumps(metrics or {})),
            )
            conn.commit()
        finally:
            conn.close()

    def get_last_execution(self, feed_id: str) -> Optional[Dict[str, Any]]:
        """Get most recent execution for a feed."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """SELECT execution_id, status, metrics, created_at
                   FROM feed_executions
                   WHERE feed_id = %s
                   ORDER BY created_at DESC LIMIT 1""",
                (feed_id,),
            )
            row = cur.fetchone()
            if row:
                return {
                    "execution_id": row[0],
                    "status": row[1],
                    "metrics": json.loads(row[2]) if isinstance(row[2], str) else row[2],
                    "created_at": str(row[3]),
                }
            return None
        finally:
            conn.close()
