"""
RAG Updater - Post-Resolution Knowledge Base Update

After an incident is resolved, this module indexes the resolution into:
1. Weaviate (vector DB) - for semantic search on future similar incidents
2. Neo4j (graph DB) - for FIXED_BY relationships

This ensures the RAG system continuously improves from resolved incidents.

Usage:
    from agents.servicenow_agent.src.rag.rag_updater import RAGUpdater

    updater = RAGUpdater()
    await updater.index_resolution(
        incident_id="INC0010001",
        description="GCP VM instance stopped",
        resolution_script="start_gcp_instance",
        resolution_notes="Restarted VM via gcloud compute instances start",
        outcome="success",
        category="compute",
        service="gcp"
    )

Author: AI Agent Platform
Version: 1.0.0
"""

import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "adminadmin")


class RAGUpdater:
    """Updates RAG knowledge base with resolved incident data."""

    def __init__(self):
        self.weaviate_url = WEAVIATE_URL
        self._embedding_service = None

    def _get_embedding_service(self):
        """Lazy-load embedding service."""
        if self._embedding_service is None:
            try:
                from agents.servicenow_agent.src.rag.embedding_service import EmbeddingService
                self._embedding_service = EmbeddingService()
            except Exception as e:
                logger.warning(f"Could not initialize embedding service: {e}")
        return self._embedding_service

    async def index_resolution(
        self,
        incident_id: str,
        description: str,
        resolution_script: str,
        resolution_notes: str = "",
        outcome: str = "success",
        category: str = "",
        service: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Index a resolved incident into Weaviate and Neo4j.

        Returns dict with indexing results.
        """
        results = {
            "incident_id": incident_id,
            "weaviate_indexed": False,
            "neo4j_indexed": False,
            "errors": [],
        }

        # 1. Index into Weaviate (vector search)
        try:
            self._index_weaviate(
                incident_id=incident_id,
                description=description,
                resolution_script=resolution_script,
                resolution_notes=resolution_notes,
                outcome=outcome,
                category=category,
                service=service,
            )
            results["weaviate_indexed"] = True
            logger.info(f"Indexed incident {incident_id} into Weaviate")
        except Exception as e:
            error_msg = f"Weaviate indexing failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

        # 2. Index into Neo4j (graph relationships)
        try:
            self._index_neo4j(
                incident_id=incident_id,
                description=description,
                resolution_script=resolution_script,
                outcome=outcome,
                category=category,
                service=service,
            )
            results["neo4j_indexed"] = True
            logger.info(f"Indexed incident {incident_id} into Neo4j")
        except Exception as e:
            error_msg = f"Neo4j indexing failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

        return results

    def _index_weaviate(
        self,
        incident_id: str,
        description: str,
        resolution_script: str,
        resolution_notes: str,
        outcome: str,
        category: str,
        service: str,
    ):
        """Add resolved incident to Weaviate for semantic search."""
        # Build the text to embed
        text = (
            f"Incident: {description}. "
            f"Resolution: {resolution_notes or resolution_script}. "
            f"Outcome: {outcome}. "
            f"Service: {service}. Category: {category}."
        )

        # Generate embedding
        vector = None
        embedding_service = self._get_embedding_service()
        if embedding_service:
            try:
                vectors = embedding_service.embed([text])
                if vectors and len(vectors) > 0:
                    vector = vectors[0].tolist() if hasattr(vectors[0], 'tolist') else list(vectors[0])
            except Exception as e:
                logger.warning(f"Embedding generation failed: {e}")

        # Build Weaviate object
        properties = {
            "incident_id": incident_id,
            "content": text,
            "description": description,
            "resolution_script": resolution_script,
            "resolution_notes": resolution_notes,
            "outcome": outcome,
            "category": category,
            "service": service,
            "indexed_at": datetime.utcnow().isoformat(),
            "source": "post_resolution",
        }

        # Add to Weaviate
        url = f"{self.weaviate_url}/v1/objects"
        payload: Dict[str, Any] = {
            "class": "ResolvedIncident",
            "properties": properties,
        }
        if vector:
            payload["vector"] = vector

        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()

    def _index_neo4j(
        self,
        incident_id: str,
        description: str,
        resolution_script: str,
        outcome: str,
        category: str,
        service: str,
    ):
        """Add FIXED_BY relationship in Neo4j graph."""
        try:
            from neo4j import GraphDatabase
        except ImportError:
            logger.warning("neo4j driver not installed, skipping graph indexing")
            return

        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

        try:
            with driver.session() as session:
                # Create or merge Incident node
                session.run(
                    """
                    MERGE (i:Incident {incident_id: $incident_id})
                    SET i.description = $description,
                        i.category = $category,
                        i.service = $service,
                        i.outcome = $outcome,
                        i.resolved_at = datetime()
                    """,
                    incident_id=incident_id,
                    description=description,
                    category=category,
                    service=service,
                    outcome=outcome,
                )

                # Create or merge Script node
                session.run(
                    """
                    MERGE (s:Script {name: $script_name})
                    SET s.last_used = datetime()
                    """,
                    script_name=resolution_script,
                )

                # Create FIXED_BY relationship
                session.run(
                    """
                    MATCH (i:Incident {incident_id: $incident_id})
                    MATCH (s:Script {name: $script_name})
                    MERGE (i)-[r:FIXED_BY]->(s)
                    SET r.outcome = $outcome,
                        r.timestamp = datetime()
                    """,
                    incident_id=incident_id,
                    script_name=resolution_script,
                    outcome=outcome,
                )

                # If service exists, link incident to service
                if service:
                    session.run(
                        """
                        MERGE (svc:Service {name: $service})
                        WITH svc
                        MATCH (i:Incident {incident_id: $incident_id})
                        MERGE (i)-[:AFFECTS]->(svc)
                        """,
                        service=service,
                        incident_id=incident_id,
                    )

                logger.info(f"Neo4j: Created FIXED_BY relationship: {incident_id} -> {resolution_script}")
        finally:
            driver.close()


# Singleton instance
_updater_instance: Optional[RAGUpdater] = None


def get_rag_updater() -> RAGUpdater:
    """Get or create singleton RAGUpdater instance."""
    global _updater_instance
    if _updater_instance is None:
        _updater_instance = RAGUpdater()
    return _updater_instance
