"""
APEX Component Registry Manager

Hybrid registry that reads from:
1. Local registry.json (fast, offline, always available)
2. PostgreSQL component tables (authoritative, synced periodically)

Responsibilities:
- Pattern selection (replaces APEXTemplateSelector)
- Component lookup (templates, utilities, spark jobs)
- Duplicate prevention (check before generating)
- Registry sync (JSON ↔ PostgreSQL)
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import structlog

from src.models.apex_models import (
    PatternCode,
    SourceCategory,
    LoadType,
    ContractType,
    Feed,
    DataContract,
    SourceRegistry,
    APEXPipelineConfig,
)

logger = structlog.get_logger()

# Path to local registry manifest
REGISTRY_JSON_PATH = Path(__file__).parent.parent.parent / "registry.json"


class RegistryManager:
    """
    Central component registry for APEX.

    Replaces APEXTemplateSelector by combining:
    - Pattern selection logic
    - Component catalog lookups
    - Duplicate detection
    - Registry sync between JSON and PostgreSQL

    Usage:
        registry = RegistryManager()
        pattern, template_path = registry.select_and_resolve(config)
    """

    # Template directory for APEX patterns
    PATTERN_TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "patterns"

    # File size threshold for BIGDATA pattern (in bytes)
    BIGDATA_FILE_SIZE_THRESHOLD = 10 * 1024 * 1024 * 1024  # 10 GB

    def __init__(self, registry_path: Optional[Path] = None) -> None:
        """
        Initialize registry manager.

        Args:
            registry_path: Override path to registry.json (for testing)
        """
        self._registry_path = registry_path or REGISTRY_JSON_PATH
        self._registry: Optional[Dict[str, Any]] = None

    @property
    def registry(self) -> Dict[str, Any]:
        """Lazy-load registry from JSON."""
        if self._registry is None:
            self._registry = self._load_registry()
        return self._registry

    def _load_registry(self) -> Dict[str, Any]:
        """Load registry.json from disk."""
        if not self._registry_path.exists():
            logger.warning("registry_json_not_found", path=str(self._registry_path))
            return {"templates": {"dag_patterns": {}}, "spark_jobs": {}, "dag_utilities": {}}

        with open(self._registry_path) as f:
            data = json.load(f)

        logger.debug("registry_loaded", patterns=len(data.get("templates", {}).get("dag_patterns", {})))
        return data

    def reload(self) -> None:
        """Force reload registry from disk."""
        self._registry = None

    # ─────────────────────────────────────────────────────────────────────────
    # PATTERN SELECTION (replaces APEXTemplateSelector)
    # ─────────────────────────────────────────────────────────────────────────

    def select_pattern(
        self,
        contract: DataContract,
        source: Optional[SourceRegistry] = None,
        feed: Optional[Feed] = None,
        file_size_bytes: Optional[int] = None,
    ) -> PatternCode:
        """
        Select the appropriate pipeline pattern.

        Resolution order:
        1. Explicit pattern_code in contract
        2. Contract type (SCD2 → P07, DATA_VAULT → P08, STAR_SCHEMA → P09)
        3. Source type characteristics
        4. Default → P01 FILE_MEDALLION

        Args:
            contract: Data contract configuration
            source: Source registry (optional)
            feed: Feed configuration (optional)
            file_size_bytes: Expected file size (optional)

        Returns:
            Selected PatternCode
        """
        # 1. Explicit pattern_code in contract
        if contract.pattern_code and contract.pattern_code != PatternCode.P01:
            logger.debug("pattern_selected_explicit", pattern=contract.pattern_code.value)
            return contract.pattern_code

        # 2. Contract type mapping
        contract_type_map = {
            ContractType.SCD2: PatternCode.P07,
            ContractType.DATA_VAULT: PatternCode.P08,
            ContractType.STAR_SCHEMA: PatternCode.P09,
        }
        if contract.contract_type in contract_type_map:
            pattern = contract_type_map[contract.contract_type]
            logger.debug("pattern_selected_contract_type", pattern=pattern.value)
            return pattern

        # 3. Source type
        if source:
            if source.source_type == SourceCategory.FILE:
                if file_size_bytes and file_size_bytes >= self.BIGDATA_FILE_SIZE_THRESHOLD:
                    logger.debug("pattern_selected_bigdata", file_size_gb=file_size_bytes / (1024**3))
                    return PatternCode.P02

            source_type_map = {
                SourceCategory.FILE: PatternCode.P01,
                SourceCategory.DATABASE: PatternCode.P03,
                SourceCategory.STREAMING: PatternCode.P05,
                SourceCategory.API: PatternCode.P06,
                SourceCategory.LEGACY: PatternCode.P04,
                SourceCategory.SAAS: PatternCode.P06,  # SAAS uses same pattern as API
            }
            if source.source_type in source_type_map:
                pattern = source_type_map[source.source_type]
                logger.debug("pattern_selected_source_type", pattern=pattern.value)
                return pattern

        # 4. Feed-level legacy indicators
        if feed:
            if feed.copybook_path or feed.encoding == "EBCDIC":
                logger.debug("pattern_selected_legacy", reason="copybook or EBCDIC")
                return PatternCode.P04

        # 5. Default
        logger.debug("pattern_selected_default", pattern=PatternCode.P01.value)
        return PatternCode.P01

    def get_template_path(self, pattern: PatternCode) -> Path:
        """
        Resolve template file path from registry.

        Args:
            pattern: Pipeline pattern code

        Returns:
            Absolute Path to template file

        Raises:
            ValueError: If template not found in registry or on disk
        """
        patterns = self.registry.get("templates", {}).get("dag_patterns", {})
        pattern_entry = patterns.get(pattern.value)

        if pattern_entry:
            rel_path = pattern_entry["path"]
            abs_path = self._registry_path.parent / rel_path
            if abs_path.exists():
                return abs_path

        # Fallback: construct from convention
        template_name = f"{pattern.value.lower()}_{self._pattern_slug(pattern)}.py.jinja2"
        fallback = self.PATTERN_TEMPLATE_DIR / template_name
        if fallback.exists():
            return fallback

        raise ValueError(f"Template not found for pattern: {pattern.value}")

    def select_and_resolve(
        self,
        config: APEXPipelineConfig,
        file_size_bytes: Optional[int] = None,
    ) -> Tuple[PatternCode, Path]:
        """
        Select pattern and resolve template path from config.

        Args:
            config: Complete APEX pipeline configuration
            file_size_bytes: Expected file size (optional)

        Returns:
            Tuple of (PatternCode, template_path)
        """
        pattern = self.select_pattern(
            contract=config.contract,
            source=config.source,
            feed=config.feed,
            file_size_bytes=file_size_bytes,
        )
        template_path = self.get_template_path(pattern)

        logger.info(
            "pattern_resolved",
            feed_id=config.feed.feed_id,
            contract_id=config.contract.contract_id,
            pattern=pattern.value,
            template=template_path.name,
        )
        return pattern, template_path

    # ─────────────────────────────────────────────────────────────────────────
    # COMPONENT LOOKUPS
    # ─────────────────────────────────────────────────────────────────────────

    def get_pattern_info(self, pattern_code: str) -> Optional[Dict[str, Any]]:
        """Get full info for a pattern from registry."""
        return self.registry.get("templates", {}).get("dag_patterns", {}).get(pattern_code)

    def get_spark_job_info(self, job_key: str) -> Optional[Dict[str, Any]]:
        """Get info for a spark job from registry."""
        return self.registry.get("spark_jobs", {}).get(job_key)

    def get_utility_info(self, category: str, class_name: str) -> Optional[Dict[str, Any]]:
        """Get info for a dag_utility class from registry."""
        cat = self.registry.get("dag_utilities", {}).get(category, {})
        classes = cat.get("classes", {})
        return classes.get(class_name)

    def get_required_variables(self, pattern_code: str) -> List[str]:
        """Get required template variables for a pattern."""
        info = self.get_pattern_info(pattern_code)
        return info.get("required_variables", []) if info else []

    def get_spark_jobs_for_pattern(self, pattern_code: str) -> List[str]:
        """Get list of spark jobs used by a pattern."""
        info = self.get_pattern_info(pattern_code)
        return info.get("spark_jobs_used", []) if info else []

    def list_patterns(self) -> List[Dict[str, Any]]:
        """List all registered patterns with metadata."""
        patterns = self.registry.get("templates", {}).get("dag_patterns", {})
        return [
            {"code": code, **info}
            for code, info in patterns.items()
        ]

    def list_utility_classes(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all registered utility classes, optionally filtered by category."""
        utils = self.registry.get("dag_utilities", {})
        results = []
        for cat_name, cat_data in utils.items():
            if category and cat_name != category:
                continue
            for cls_name, cls_info in cat_data.get("classes", {}).items():
                results.append({
                    "category": cat_name,
                    "class_name": cls_name,
                    "file": cls_info.get("file"),
                    "methods": cls_info.get("methods", []),
                    "description": cls_info.get("description"),
                })
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # DUPLICATE DETECTION
    # ─────────────────────────────────────────────────────────────────────────

    def component_exists(self, component_type: str, name: str) -> bool:
        """
        Check if a component already exists in the registry.

        Args:
            component_type: "pattern", "spark_job", or "utility"
            name: Component name or key

        Returns:
            True if component exists
        """
        if component_type == "pattern":
            return name in self.registry.get("templates", {}).get("dag_patterns", {})
        elif component_type == "spark_job":
            return name in self.registry.get("spark_jobs", {})
        elif component_type == "utility":
            for cat in self.registry.get("dag_utilities", {}).values():
                if name in cat.get("classes", {}):
                    return True
            return False
        return False

    def find_pattern_for(
        self,
        source_type: str,
        contract_type: str = "STANDARD",
        load_type: str = "FULL",
    ) -> Optional[str]:
        """
        Find a matching pattern code from registry metadata.

        Args:
            source_type: e.g. "FILE", "DATABASE"
            contract_type: e.g. "STANDARD", "SCD2"
            load_type: e.g. "FULL", "INCREMENTAL"

        Returns:
            Pattern code string or None
        """
        patterns = self.registry.get("templates", {}).get("dag_patterns", {})
        candidates = []

        for code, info in patterns.items():
            if source_type in info.get("source_types", []) and \
               contract_type in info.get("contract_types", []) and \
               load_type in info.get("load_types", []):
                candidates.append((code, info.get("selection_priority", 0)))

        if not candidates:
            return None

        # Return highest priority
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]

    # ─────────────────────────────────────────────────────────────────────────
    # PATTERN DESCRIPTION (for UI/logging)
    # ─────────────────────────────────────────────────────────────────────────

    def get_pattern_description(self, pattern: PatternCode) -> str:
        """Get human-readable description of a pattern."""
        info = self.get_pattern_info(pattern.value)
        if info:
            return info.get("description", "Unknown pattern")
        return "Unknown pattern"

    def get_available_patterns(self) -> List[Dict[str, str]]:
        """Get list of all available patterns with descriptions."""
        results = []
        for code, info in self.registry.get("templates", {}).get("dag_patterns", {}).items():
            template_path = self._registry_path.parent / info["path"]
            results.append({
                "code": code,
                "name": info["name"],
                "description": info.get("description", ""),
                "template": info["path"],
                "available": template_path.exists(),
            })
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # INTERNAL HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _pattern_slug(pattern: PatternCode) -> str:
        """Convert PatternCode enum name to slug for filename."""
        # P01_FILE_MEDALLION → file_medallion
        name = pattern.name
        prefix = name.split("_", 1)
        return prefix[1].lower() if len(prefix) > 1 else name.lower()
