"""
APEX Data Agent — Data Catalog Repository

Provides CRUD and search for data assets, business glossary, and tag taxonomy.
Auto-registers assets after zone transitions for complete data discovery.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger()


class CatalogRepository:
    """
    Manages the data catalog: assets, business terms, and tags.

    Tables:
    - data_asset: Every table/dataset across zones
    - business_term: Business glossary entries
    - tag_taxonomy: Hierarchical classification tags
    """

    def __init__(self, connection_string: Optional[str] = None):
        """Initialize with PostgreSQL connection."""
        import psycopg2
        import os

        conn_str = connection_string or os.getenv("APEX_METADATA_DB_URL")
        self.conn = psycopg2.connect(conn_str)
        self.conn.autocommit = False

    # ═══════════════════════════════════════════════════════════════════════
    # DATA ASSET OPERATIONS
    # ═══════════════════════════════════════════════════════════════════════

    def register_asset(
        self,
        asset_name: str,
        asset_type: str,
        zone_level: str,
        feed_id: Optional[str] = None,
        contract_id: Optional[str] = None,
        dataset_path: Optional[str] = None,
        schema_json: Optional[Dict] = None,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        owner: Optional[str] = None,
        domain: Optional[str] = None,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None,
        quality_score: Optional[float] = None,
    ) -> str:
        """
        Register or update a data asset in the catalog.

        Uses UPSERT on (asset_name, zone_level) to keep catalog current.
        """
        asset_id = str(uuid.uuid4())
        now = datetime.utcnow()

        query = """
            INSERT INTO data_asset (
                asset_id, asset_name, asset_type, zone_level, feed_id,
                contract_id, dataset_path, schema_json, row_count, size_bytes,
                owner, domain, tags, description, quality_score,
                last_refreshed_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (asset_name, zone_level)
                DO UPDATE SET
                    schema_json = EXCLUDED.schema_json,
                    row_count = EXCLUDED.row_count,
                    size_bytes = EXCLUDED.size_bytes,
                    tags = EXCLUDED.tags,
                    quality_score = EXCLUDED.quality_score,
                    last_refreshed_at = EXCLUDED.last_refreshed_at,
                    updated_at = EXCLUDED.updated_at
            RETURNING asset_id
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, [
                    asset_id, asset_name, asset_type, zone_level, feed_id,
                    contract_id, dataset_path,
                    json.dumps(schema_json) if schema_json else None,
                    row_count, size_bytes, owner, domain,
                    json.dumps(tags or []),
                    description, quality_score, now, now, now,
                ])
                result = cur.fetchone()
                self.conn.commit()
                return str(result[0]) if result else asset_id
        except Exception as e:
            self.conn.rollback()
            logger.error("catalog_register_asset_failed", asset_name=asset_name, error=str(e))
            raise

    def search_assets(
        self,
        query: Optional[str] = None,
        zone: Optional[str] = None,
        domain: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Search data assets with full-text search and filters.

        Args:
            query: Free-text search across name, description, domain
            zone: Filter by zone level (BRONZE, SILVER, GOLD, etc.)
            domain: Filter by business domain
            tags: Filter by tags (any match)
            limit: Max results
        """
        conditions = ["da.is_active = true"]
        params = []

        if query:
            conditions.append(
                "to_tsvector('english', coalesce(da.asset_name, '') || ' ' || "
                "coalesce(da.description, '') || ' ' || coalesce(da.domain, '')) "
                "@@ plainto_tsquery('english', %s)"
            )
            params.append(query)

        if zone:
            conditions.append("da.zone_level = %s")
            params.append(zone)

        if domain:
            conditions.append("da.domain = %s")
            params.append(domain)

        if tags:
            conditions.append("da.tags ?| %s")
            params.append(tags)

        params.append(limit)

        sql = f"""
            SELECT asset_id, asset_name, asset_type, zone_level, feed_id,
                   dataset_path, domain, tags, description, row_count,
                   quality_score, last_refreshed_at
            FROM data_asset da
            WHERE {' AND '.join(conditions)}
            ORDER BY da.updated_at DESC
            LIMIT %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_asset_by_path(self, dataset_path: str) -> Optional[Dict[str, Any]]:
        """Look up asset by its GCS/BQ path."""
        query = """
            SELECT asset_id, asset_name, asset_type, zone_level, feed_id,
                   dataset_path, domain, tags, schema_json, row_count
            FROM data_asset
            WHERE dataset_path = %s AND is_active = true
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(query, [dataset_path])
            row = cur.fetchone()
            if row:
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))
        return None

    def get_asset_lineage(self, asset_id: str) -> Dict[str, List[Dict]]:
        """Get upstream and downstream lineage for an asset."""
        asset = self._get_asset(asset_id)
        if not asset:
            return {"upstream": [], "downstream": []}

        asset_name = asset["asset_name"]

        upstream_sql = """
            SELECT DISTINCT dl.source_entity, da.asset_id, da.zone_level, da.domain
            FROM data_lineage dl
            LEFT JOIN data_asset da ON da.asset_name = dl.source_entity AND da.is_active = true
            WHERE dl.target_entity = %s
        """
        downstream_sql = """
            SELECT DISTINCT dl.target_entity, da.asset_id, da.zone_level, da.domain
            FROM data_lineage dl
            LEFT JOIN data_asset da ON da.asset_name = dl.target_entity AND da.is_active = true
            WHERE dl.source_entity = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(upstream_sql, [asset_name])
            upstream = [{"entity": r[0], "asset_id": str(r[1]) if r[1] else None,
                         "zone": r[2], "domain": r[3]} for r in cur.fetchall()]

            cur.execute(downstream_sql, [asset_name])
            downstream = [{"entity": r[0], "asset_id": str(r[1]) if r[1] else None,
                           "zone": r[2], "domain": r[3]} for r in cur.fetchall()]

        return {"upstream": upstream, "downstream": downstream}

    def _get_asset(self, asset_id: str) -> Optional[Dict]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT asset_id, asset_name FROM data_asset WHERE asset_id = %s", [asset_id])
            row = cur.fetchone()
            if row:
                return {"asset_id": str(row[0]), "asset_name": row[1]}
        return None

    # ═══════════════════════════════════════════════════════════════════════
    # BUSINESS GLOSSARY
    # ═══════════════════════════════════════════════════════════════════════

    def register_business_term(
        self,
        term_name: str,
        definition: str,
        domain: Optional[str] = None,
        owner: Optional[str] = None,
        synonyms: Optional[List[str]] = None,
        related_asset_ids: Optional[List[str]] = None,
    ) -> str:
        """Register a business glossary term."""
        term_id = str(uuid.uuid4())
        query = """
            INSERT INTO business_term (
                term_id, term_name, definition, domain, owner,
                synonyms, related_assets, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (term_name)
                DO UPDATE SET definition = EXCLUDED.definition,
                              domain = EXCLUDED.domain,
                              updated_at = EXCLUDED.updated_at
            RETURNING term_id
        """
        now = datetime.utcnow()
        with self.conn.cursor() as cur:
            cur.execute(query, [
                term_id, term_name, definition, domain, owner,
                json.dumps(synonyms or []),
                json.dumps(related_asset_ids or []),
                now, now,
            ])
            result = cur.fetchone()
            self.conn.commit()
            return str(result[0]) if result else term_id

    def search_business_terms(self, query: str, limit: int = 20) -> List[Dict]:
        """Search business glossary."""
        sql = """
            SELECT term_id, term_name, definition, domain, owner, synonyms
            FROM business_term
            WHERE is_active = true
              AND to_tsvector('english', coalesce(term_name, '') || ' ' || coalesce(definition, ''))
                  @@ plainto_tsquery('english', %s)
            ORDER BY term_name
            LIMIT %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, [query, limit])
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
