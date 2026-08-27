"""
StoredProcReader — Fetch stored procedure definitions from live databases.

Supports SQL Server, Oracle, and PostgreSQL. Falls back to static .sql file
scanning when the DB is unreachable. Credential resolution mirrors
connection_test_agent.py: query connection_registry, resolve secret via
the credentials_secret_path field.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ProcParameter:
    name: str
    data_type: str
    direction: str = "INPUT"   # INPUT | OUTPUT | INOUT
    default_value: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "direction": self.direction,
            "default_value": self.default_value,
        }


@dataclass
class RawObjectReference:
    referencing_schema: str
    referencing_name: str
    referenced_schema: str
    referenced_name: str
    reference_type: str = "CALLS"  # CALLS | SELECTS_FROM | INSERTS_INTO | UPDATES | DELETES_FROM

    def to_dict(self) -> Dict[str, Any]:
        return {
            "referencing_schema": self.referencing_schema,
            "referencing_name": self.referencing_name,
            "referenced_schema": self.referenced_schema,
            "referenced_name": self.referenced_name,
            "reference_type": self.reference_type,
        }


@dataclass
class StoredProcDefinition:
    schema: str
    name: str
    db_platform: str           # SQLSERVER | ORACLE | POSTGRESQL
    object_type: str           # PROCEDURE | FUNCTION | VIEW | TRIGGER | PACKAGE | PACKAGE_BODY
    definition_text: Optional[str] = None
    parameters: List[ProcParameter] = field(default_factory=list)
    referenced_objects: List[RawObjectReference] = field(default_factory=list)
    is_encrypted: bool = False
    extraction_source: str = "LIVE_DB"   # LIVE_DB | STATIC_FILES
    char_count: int = 0
    error_message: Optional[str] = None

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "name": self.name,
            "fqn": self.fqn,
            "db_platform": self.db_platform,
            "object_type": self.object_type,
            "definition_text": self.definition_text,
            "parameters": [p.to_dict() for p in self.parameters],
            "referenced_objects": [r.to_dict() for r in self.referenced_objects],
            "is_encrypted": self.is_encrypted,
            "extraction_source": self.extraction_source,
            "char_count": self.char_count,
            "error_message": self.error_message,
        }


# ---------------------------------------------------------------------------
# SQL queries per platform
# ---------------------------------------------------------------------------

_SQLSERVER_LIST_SQL = """
SELECT
    SCHEMA_NAME(o.schema_id)                        AS object_schema,
    o.name                                          AS object_name,
    o.type_desc                                     AS object_type,
    OBJECTPROPERTY(o.object_id, 'IsEncrypted')      AS is_encrypted
FROM sys.objects o
WHERE o.type IN ('P', 'FN', 'TF', 'IF', 'V', 'TR')
  AND SCHEMA_NAME(o.schema_id) LIKE :schema_filter
  AND o.name LIKE :name_filter
"""

_SQLSERVER_DEFINITION_SQL = "SELECT OBJECT_DEFINITION(OBJECT_ID(N'{fqn}'))"

_SQLSERVER_PARAMS_SQL = """
SELECT
    p.name                          AS param_name,
    TYPE_NAME(p.user_type_id)       AS data_type,
    p.is_output                     AS is_output
FROM sys.parameters p
WHERE p.object_id = OBJECT_ID(N'{fqn}')
  AND p.parameter_id > 0
ORDER BY p.parameter_id
"""

_SQLSERVER_DEPS_SQL = """
SELECT
    SCHEMA_NAME(ref.schema_id)      AS referencing_schema,
    ref.name                        AS referencing_name,
    d.referenced_schema_name        AS referenced_schema,
    d.referenced_entity_name        AS referenced_name,
    CASE d.referenced_class
        WHEN 1 THEN 'SELECTS_FROM'
        ELSE 'CALLS'
    END                             AS reference_type
FROM sys.sql_expression_dependencies d
JOIN sys.objects ref ON ref.object_id = d.referencing_id
WHERE d.referencing_id IN ({placeholders})
"""

_ORACLE_LIST_SQL = """
SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
FROM ALL_OBJECTS
WHERE OWNER LIKE :schema_filter
  AND OBJECT_NAME LIKE :name_filter
  AND OBJECT_TYPE IN ('PROCEDURE','FUNCTION','VIEW','TRIGGER','PACKAGE','PACKAGE BODY')
"""

_ORACLE_SOURCE_SQL = """
SELECT TEXT FROM ALL_SOURCE
WHERE OWNER = :owner AND NAME = :name AND TYPE = :obj_type
ORDER BY LINE
"""

_ORACLE_PARAMS_SQL = """
SELECT ARGUMENT_NAME, DATA_TYPE, IN_OUT, DEFAULT_VALUE
FROM ALL_ARGUMENTS
WHERE OWNER = :owner AND OBJECT_NAME = :name AND SUBPROGRAM_ID = 0
ORDER BY POSITION
"""

_ORACLE_DEPS_SQL = """
SELECT OWNER, NAME, REFERENCED_OWNER, REFERENCED_NAME, DEPENDENCY_TYPE
FROM ALL_DEPENDENCIES
WHERE OWNER = :schema AND NAME IN ({placeholders})
  AND TYPE IN ('PROCEDURE','FUNCTION','VIEW')
"""

_PG_LIST_SQL = """
SELECT n.nspname AS schema, p.proname AS name,
       CASE p.prokind WHEN 'p' THEN 'PROCEDURE' WHEN 'f' THEN 'FUNCTION' ELSE 'UNKNOWN' END AS obj_type
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname LIKE %(schema_filter)s
  AND p.proname LIKE %(name_filter)s
"""

_PG_VIEWS_SQL = """
SELECT table_schema AS schema, table_name AS name, 'VIEW' AS obj_type
FROM information_schema.views
WHERE table_schema LIKE %(schema_filter)s
  AND table_name LIKE %(name_filter)s
"""

_PG_DEFINITION_SQL = "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = %(schema)s AND p.proname = %(name)s LIMIT 1"

_PG_PARAMS_SQL = """
SELECT
    unnest(p.proargnames)           AS param_name,
    unnest(p.proargtypes::int[])    AS type_oid,
    'INPUT'                         AS direction
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = %(schema)s AND p.proname = %(name)s
LIMIT 1
"""

_PG_DEPS_SQL = """
SELECT
    n.nspname                       AS referencing_schema,
    p.proname                       AS referencing_name,
    dn.nspname                      AS referenced_schema,
    dc.relname                      AS referenced_name,
    'CALLS'                         AS reference_type
FROM pg_depend dep
JOIN pg_proc p ON p.oid = dep.objid
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_class dc ON dc.oid = dep.refobjid
JOIN pg_namespace dn ON dn.oid = dc.relnamespace
WHERE dep.classid = 'pg_proc'::regclass
  AND dep.refclassid = 'pg_class'::regclass
  AND dep.deptype = 'n'
  AND p.proname = ANY(%(names)s)
  AND n.nspname = %(schema)s
"""

# ---------------------------------------------------------------------------
# Static fallback regexes
# ---------------------------------------------------------------------------

_STATIC_OBJECT_HEADER = re.compile(
    r"CREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?"
    r"(PROCEDURE|FUNCTION|VIEW|TRIGGER|PACKAGE\s+BODY|PACKAGE)\s+"
    r"(?:\[?(?P<schema>\w+)\]?\.)?\[?(?P<name>\w+)\]?",
    re.IGNORECASE,
)

_STATIC_EXEC_REF = re.compile(r"\bEXEC(?:UTE)?\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?", re.IGNORECASE)
_STATIC_FROM_REF = re.compile(r"\bFROM\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?", re.IGNORECASE)
_STATIC_INTO_REF = re.compile(r"\bINSERT\s+INTO\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?", re.IGNORECASE)
_STATIC_UPDATE_REF = re.compile(r"\bUPDATE\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?", re.IGNORECASE)


# ---------------------------------------------------------------------------
# StoredProcReader
# ---------------------------------------------------------------------------

class StoredProcReader:
    """
    Fetch stored procedure / function / view definitions from a live database.

    Platform detection is based on db_platform set in the connection registry
    record ("SQLSERVER", "ORACLE", "POSTGRESQL").
    """

    def __init__(
        self,
        connection_record: Dict[str, Any],
        timeout_seconds: int = 60,
        max_definition_length: int = 100_000,
    ) -> None:
        self._conn_record = connection_record
        self._timeout = timeout_seconds
        self._max_def_length = max_definition_length
        self._platform = (connection_record.get("db_platform") or "").upper()
        self._log = logger.bind(
            platform=self._platform,
            host=connection_record.get("host", ""),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_definitions(
        self,
        schema_filter: str = "%",
        name_filter: str = "%",
    ) -> List[StoredProcDefinition]:
        """Return definitions for all matching objects in the source DB."""
        t0 = time.monotonic()
        try:
            if self._platform == "SQLSERVER":
                results = self._sqlserver_list(schema_filter, name_filter)
            elif self._platform == "ORACLE":
                results = self._oracle_list(schema_filter, name_filter)
            elif self._platform == "POSTGRESQL":
                results = self._pg_list(schema_filter, name_filter)
            else:
                raise ValueError(f"Unsupported platform: {self._platform}")
            self._log.info(
                "definitions_fetched",
                count=len(results),
                elapsed_s=round(time.monotonic() - t0, 2),
            )
            return results
        except Exception as exc:
            self._log.error("fetch_definitions_failed", error=str(exc))
            raise

    def static_fallback(
        self,
        sql_root_dir: Path,
        name_filter: str = "*",
    ) -> List[StoredProcDefinition]:
        """
        Walk .sql files in sql_root_dir and extract object definitions using
        regex matching. Used when the live DB is unreachable.
        """
        results: List[StoredProcDefinition] = []
        pattern = name_filter.replace("%", "*")
        for sql_file in sql_root_dir.rglob("*.sql"):
            try:
                text = sql_file.read_text(encoding="utf-8", errors="replace")
                results.extend(self._parse_static_file(text, str(sql_file)))
            except Exception as exc:
                self._log.warning("static_file_parse_error", file=str(sql_file), error=str(exc))
        # Filter by name pattern
        if name_filter not in ("%", "*"):
            results = [r for r in results if _match_like(r.name, name_filter)]
        self._log.info("static_fallback_complete", files_scanned=sum(1 for _ in sql_root_dir.rglob("*.sql")), extracted=len(results))
        return results

    # ------------------------------------------------------------------
    # SQL Server implementation
    # ------------------------------------------------------------------

    def _sqlserver_list(self, schema_filter: str, name_filter: str) -> List[StoredProcDefinition]:
        import pyodbc
        conn = self._get_connection()
        cur = conn.cursor()
        results: List[StoredProcDefinition] = []
        try:
            cur.execute(
                _SQLSERVER_LIST_SQL.replace(":schema_filter", "?").replace(":name_filter", "?"),
                (schema_filter, name_filter),
            )
            rows = cur.fetchall()
            for row in rows:
                schema, name, raw_type, is_enc = row
                obj_type = _normalize_sqlserver_type(raw_type)
                fqn = f"{schema}.{name}"
                defn_text = None
                encrypted = bool(is_enc)

                if not encrypted:
                    cur.execute(_SQLSERVER_DEFINITION_SQL.format(fqn=fqn.replace("'", "''")))
                    defn_row = cur.fetchone()
                    defn_text = defn_row[0] if defn_row else None
                    if defn_text and len(defn_text) > self._max_def_length:
                        defn_text = defn_text[: self._max_def_length] + "\n-- [TRUNCATED]"

                params = self._sqlserver_params(cur, fqn)
                refs = self._sqlserver_deps(cur, fqn)

                results.append(StoredProcDefinition(
                    schema=schema,
                    name=name,
                    db_platform="SQLSERVER",
                    object_type=obj_type,
                    definition_text=defn_text,
                    parameters=params,
                    referenced_objects=refs,
                    is_encrypted=encrypted,
                    extraction_source="LIVE_DB",
                    char_count=len(defn_text) if defn_text else 0,
                ))
        finally:
            conn.close()
        return results

    def _sqlserver_params(self, cur: Any, fqn: str) -> List[ProcParameter]:
        cur.execute(_SQLSERVER_PARAMS_SQL.format(fqn=fqn.replace("'", "''")))
        params = []
        for row in cur.fetchall():
            name, dtype, is_out = row
            params.append(ProcParameter(
                name=name,
                data_type=dtype or "UNKNOWN",
                direction="OUTPUT" if is_out else "INPUT",
            ))
        return params

    def _sqlserver_deps(self, cur: Any, fqn: str) -> List[RawObjectReference]:
        schema, name = fqn.split(".", 1)
        cur.execute(
            "SELECT object_id FROM sys.objects WHERE schema_id = SCHEMA_ID(?) AND name = ?",
            (schema, name),
        )
        row = cur.fetchone()
        if not row:
            return []
        obj_id = row[0]
        cur.execute(
            _SQLSERVER_DEPS_SQL.format(placeholders="?"),
            (obj_id,),
        )
        refs = []
        for r in cur.fetchall():
            ref_schema, ref_name, dep_schema, dep_name, ref_type = r
            refs.append(RawObjectReference(
                referencing_schema=ref_schema or schema,
                referencing_name=ref_name or name,
                referenced_schema=dep_schema or schema,
                referenced_name=dep_name,
                reference_type=ref_type,
            ))
        return refs

    # ------------------------------------------------------------------
    # Oracle implementation
    # ------------------------------------------------------------------

    def _oracle_list(self, schema_filter: str, name_filter: str) -> List[StoredProcDefinition]:
        conn = self._get_connection()
        cur = conn.cursor()
        results: List[StoredProcDefinition] = []
        try:
            cur.execute(
                _ORACLE_LIST_SQL.replace(":schema_filter", ":1").replace(":name_filter", ":2"),
                (schema_filter.upper(), name_filter.upper()),
            )
            rows = cur.fetchall()
            for row in rows:
                owner, name, raw_type = row
                obj_type = raw_type.replace(" ", "_")
                source_lines: List[str] = []

                cur.execute(
                    _ORACLE_SOURCE_SQL.replace(":owner", ":1").replace(":name", ":2").replace(":obj_type", ":3"),
                    (owner, name, raw_type),
                )
                for src_row in cur.fetchall():
                    source_lines.append(src_row[0])
                defn_text = "".join(source_lines)
                if len(defn_text) > self._max_def_length:
                    defn_text = defn_text[: self._max_def_length] + "\n-- [TRUNCATED]"

                params = self._oracle_params(cur, owner, name)
                refs = self._oracle_deps(cur, owner, name)

                results.append(StoredProcDefinition(
                    schema=owner,
                    name=name,
                    db_platform="ORACLE",
                    object_type=obj_type,
                    definition_text=defn_text or None,
                    parameters=params,
                    referenced_objects=refs,
                    is_encrypted=False,
                    extraction_source="LIVE_DB",
                    char_count=len(defn_text),
                ))
        finally:
            conn.close()
        return results

    def _oracle_params(self, cur: Any, owner: str, name: str) -> List[ProcParameter]:
        cur.execute(
            _ORACLE_PARAMS_SQL.replace(":owner", ":1").replace(":name", ":2"),
            (owner, name),
        )
        params = []
        for row in cur.fetchall():
            arg_name, dtype, in_out, default_val = row
            if not arg_name:
                continue
            direction = {"IN": "INPUT", "OUT": "OUTPUT", "IN/OUT": "INOUT"}.get(in_out, "INPUT")
            params.append(ProcParameter(
                name=arg_name,
                data_type=dtype or "UNKNOWN",
                direction=direction,
                default_value=default_val,
            ))
        return params

    def _oracle_deps(self, cur: Any, owner: str, name: str) -> List[RawObjectReference]:
        cur.execute(
            _ORACLE_DEPS_SQL.format(placeholders=":1").replace(":schema", ":2"),
            (name, owner),
        )
        refs = []
        for row in cur.fetchall():
            ref_owner, ref_name, dep_owner, dep_name, dep_type = row
            refs.append(RawObjectReference(
                referencing_schema=ref_owner,
                referencing_name=ref_name,
                referenced_schema=dep_owner or owner,
                referenced_name=dep_name,
                reference_type="CALLS",
            ))
        return refs

    # ------------------------------------------------------------------
    # PostgreSQL implementation
    # ------------------------------------------------------------------

    def _pg_list(self, schema_filter: str, name_filter: str) -> List[StoredProcDefinition]:
        import psycopg2
        conn = self._get_connection()
        cur = conn.cursor()
        results: List[StoredProcDefinition] = []
        try:
            cur.execute(_PG_LIST_SQL, {"schema_filter": schema_filter, "name_filter": name_filter})
            proc_rows = cur.fetchall()
            cur.execute(_PG_VIEWS_SQL, {"schema_filter": schema_filter, "name_filter": name_filter})
            view_rows = cur.fetchall()
            all_rows = proc_rows + view_rows

            for row in all_rows:
                schema, name, obj_type = row
                defn_text = None

                if obj_type in ("PROCEDURE", "FUNCTION"):
                    cur.execute(_PG_DEFINITION_SQL, {"schema": schema, "name": name})
                    defn_row = cur.fetchone()
                    defn_text = defn_row[0] if defn_row else None
                else:
                    cur.execute(
                        "SELECT view_definition FROM information_schema.views WHERE table_schema = %s AND table_name = %s",
                        (schema, name),
                    )
                    defn_row = cur.fetchone()
                    defn_text = defn_row[0] if defn_row else None

                if defn_text and len(defn_text) > self._max_def_length:
                    defn_text = defn_text[: self._max_def_length] + "\n-- [TRUNCATED]"

                params = self._pg_params(cur, schema, name) if obj_type != "VIEW" else []
                refs = self._pg_deps(cur, schema, name)

                results.append(StoredProcDefinition(
                    schema=schema,
                    name=name,
                    db_platform="POSTGRESQL",
                    object_type=obj_type,
                    definition_text=defn_text or None,
                    parameters=params,
                    referenced_objects=refs,
                    is_encrypted=False,
                    extraction_source="LIVE_DB",
                    char_count=len(defn_text) if defn_text else 0,
                ))
        finally:
            conn.close()
        return results

    def _pg_params(self, cur: Any, schema: str, name: str) -> List[ProcParameter]:
        # pg_get_function_arguments gives a cleaner result than unnest
        cur.execute(
            "SELECT pg_get_function_arguments(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=%s AND p.proname=%s LIMIT 1",
            (schema, name),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return []
        params = []
        for part in row[0].split(","):
            part = part.strip()
            if not part:
                continue
            tokens = part.split()
            direction = "INPUT"
            if tokens and tokens[0].upper() in ("IN", "OUT", "INOUT", "VARIADIC"):
                direction = {"IN": "INPUT", "OUT": "OUTPUT", "INOUT": "INOUT", "VARIADIC": "INPUT"}.get(tokens[0].upper(), "INPUT")
                tokens = tokens[1:]
            if len(tokens) >= 2:
                param_name, dtype = tokens[0], " ".join(tokens[1:])
            elif len(tokens) == 1:
                param_name, dtype = "", tokens[0]
            else:
                continue
            params.append(ProcParameter(name=param_name, data_type=dtype, direction=direction))
        return params

    def _pg_deps(self, cur: Any, schema: str, name: str) -> List[RawObjectReference]:
        cur.execute(_PG_DEPS_SQL, {"names": [name], "schema": schema})
        refs = []
        for row in cur.fetchall():
            ref_schema, ref_name, dep_schema, dep_name, ref_type = row
            refs.append(RawObjectReference(
                referencing_schema=ref_schema,
                referencing_name=ref_name,
                referenced_schema=dep_schema,
                referenced_name=dep_name,
                reference_type=ref_type,
            ))
        return refs

    # ------------------------------------------------------------------
    # Static .sql file parsing
    # ------------------------------------------------------------------

    def _parse_static_file(self, text: str, source_path: str) -> List[StoredProcDefinition]:
        results: List[StoredProcDefinition] = []
        # Split on GO or ; to find individual object definitions
        segments = re.split(r"\bGO\b", text, flags=re.IGNORECASE)
        for segment in segments:
            m = _STATIC_OBJECT_HEADER.search(segment)
            if not m:
                continue
            obj_type = m.group(1).upper().replace(" ", "_")
            schema = m.group("schema") or "dbo"
            name = m.group("name")
            refs = _extract_static_refs(segment, schema, name)
            defn = segment.strip()
            if len(defn) > self._max_def_length:
                defn = defn[: self._max_def_length] + "\n-- [TRUNCATED]"
            results.append(StoredProcDefinition(
                schema=schema,
                name=name,
                db_platform=self._platform or "SQLSERVER",
                object_type=obj_type,
                definition_text=defn,
                parameters=[],
                referenced_objects=refs,
                is_encrypted=False,
                extraction_source="STATIC_FILES",
                char_count=len(defn),
            ))
        return results

    # ------------------------------------------------------------------
    # Connection resolution
    # ------------------------------------------------------------------

    def _get_connection(self) -> Any:
        """
        Build a live DB connection from the connection_record dict.
        connection_record comes from connection_registry + optional Secret Manager
        resolution (already resolved by the caller via database_source.get_jdbc_connection).
        """
        record = self._conn_record
        platform = self._platform
        host = record.get("host", "")
        port = record.get("port")
        database = record.get("database", "")
        user = record.get("user", record.get("username", ""))
        password = record.get("password", "")

        if platform == "SQLSERVER":
            import pyodbc
            port = port or 1433
            driver = record.get("driver", "ODBC Driver 18 for SQL Server")
            dsn = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={user};PWD={password};"
                "Encrypt=yes;TrustServerCertificate=yes;"
                f"Connection Timeout={self._timeout};"
            )
            return pyodbc.connect(dsn, timeout=self._timeout)

        elif platform == "ORACLE":
            import cx_Oracle
            port = port or 1521
            service = record.get("service_name") or record.get("sid") or database
            dsn = cx_Oracle.makedsn(host, port, service_name=service)
            return cx_Oracle.connect(user=user, password=password, dsn=dsn)

        elif platform == "POSTGRESQL":
            import psycopg2
            port = port or 5432
            return psycopg2.connect(
                host=host, port=port, dbname=database,
                user=user, password=password,
                connect_timeout=self._timeout,
            )
        else:
            raise ValueError(f"Cannot create connection for platform: {platform}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_sqlserver_type(raw: str) -> str:
    mapping = {
        "SQL_STORED_PROCEDURE": "PROCEDURE",
        "SQL_SCALAR_FUNCTION": "FUNCTION",
        "SQL_TABLE_VALUED_FUNCTION": "FUNCTION",
        "SQL_INLINE_TABLE_VALUED_FUNCTION": "FUNCTION",
        "VIEW": "VIEW",
        "SQL_TRIGGER": "TRIGGER",
    }
    return mapping.get((raw or "").upper(), raw or "PROCEDURE")


def _match_like(value: str, pattern: str) -> bool:
    """Simple SQL LIKE matcher: % is wildcard."""
    regex = re.escape(pattern).replace(r"\%", ".*").replace(r"\_", ".")
    return bool(re.fullmatch(regex, value, re.IGNORECASE))


def _extract_static_refs(body: str, referencing_schema: str, referencing_name: str) -> List[RawObjectReference]:
    refs: List[RawObjectReference] = []
    seen: set = set()

    def add(dep_schema: Optional[str], dep_name: str, ref_type: str) -> None:
        dep_schema = dep_schema or referencing_schema
        key = (dep_schema, dep_name, ref_type)
        if key not in seen and dep_name.lower() != referencing_name.lower():
            seen.add(key)
            refs.append(RawObjectReference(
                referencing_schema=referencing_schema,
                referencing_name=referencing_name,
                referenced_schema=dep_schema,
                referenced_name=dep_name,
                reference_type=ref_type,
            ))

    for m in _STATIC_EXEC_REF.finditer(body):
        add(m.group(1), m.group(2), "CALLS")
    for m in _STATIC_FROM_REF.finditer(body):
        add(m.group(1), m.group(2), "SELECTS_FROM")
    for m in _STATIC_INTO_REF.finditer(body):
        add(m.group(1), m.group(2), "INSERTS_INTO")
    for m in _STATIC_UPDATE_REF.finditer(body):
        add(m.group(1), m.group(2), "UPDATES")
    return refs
