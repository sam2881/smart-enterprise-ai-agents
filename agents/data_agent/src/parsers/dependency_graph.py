"""
DependencyGraph — topological sort + batch grouping for stored procedure migration.

Nodes are stored procedure FQNs (schema.name). Edges represent one object
calling or referencing another. Kahn's BFS algorithm produces a topological
ordering. Objects at the same topo_level can execute in parallel (Airflow
TaskGroup). Leaf nodes (no outgoing calls) get topo_level = 0.
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

import structlog

from src.parsers.stored_proc_reader import RawObjectReference, StoredProcDefinition

logger = structlog.get_logger(__name__)


class CycleDetectedError(Exception):
    """Raised when a dependency cycle is found and strict mode is on."""
    def __init__(self, cycle: List[str]) -> None:
        self.cycle = cycle
        super().__init__(f"Dependency cycle detected: {' → '.join(cycle)}")


class DependencyGraph:
    """
    Directed graph of stored procedure dependencies.

    Edge direction: parent → child means parent CALLS child.
    Topological level 0 = deepest leaf (no outgoing edges / no dependencies).
    Higher levels depend on lower-level objects and must run after them.
    """

    def __init__(self) -> None:
        # fqn → StoredProcDefinition
        self._nodes: Dict[str, StoredProcDefinition] = {}
        # fqn → set of fqns this node calls/depends on
        self._adj: Dict[str, Set[str]] = defaultdict(set)
        # fqn → set of fqns that call this node (reverse)
        self._radj: Dict[str, Set[str]] = defaultdict(set)
        # fqn → topo level (0 = leaf, higher = more upstream)
        self._levels: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Build phase
    # ------------------------------------------------------------------

    def add_node(self, proc: StoredProcDefinition) -> None:
        fqn = proc.fqn
        if fqn not in self._nodes:
            self._nodes[fqn] = proc
            _ = self._adj[fqn]   # ensure key exists

    def add_edge(self, ref: RawObjectReference) -> None:
        parent = f"{ref.referencing_schema}.{ref.referencing_name}"
        child = f"{ref.referenced_schema}.{ref.referenced_name}"
        if parent == child:
            return  # skip self-references
        # Auto-register dangling nodes (referenced objects not yet fetched)
        if parent not in self._nodes:
            from src.parsers.stored_proc_reader import StoredProcDefinition
            self._nodes[parent] = StoredProcDefinition(
                schema=ref.referencing_schema,
                name=ref.referencing_name,
                db_platform="UNKNOWN",
                object_type="PROCEDURE",
            )
        if child not in self._nodes:
            from src.parsers.stored_proc_reader import StoredProcDefinition
            self._nodes[child] = StoredProcDefinition(
                schema=ref.referenced_schema,
                name=ref.referenced_name,
                db_platform="UNKNOWN",
                object_type="PROCEDURE",
            )
        self._adj[parent].add(child)
        self._radj[child].add(parent)

    def load_from_definitions(self, definitions: List[StoredProcDefinition]) -> None:
        """Add all nodes and their embedded reference edges."""
        for proc in definitions:
            self.add_node(proc)
        for proc in definitions:
            for ref in proc.referenced_objects:
                self.add_edge(ref)

    # ------------------------------------------------------------------
    # Analysis
    # ------------------------------------------------------------------

    def topological_sort(self, strict: bool = False) -> List[str]:
        """
        Kahn's BFS topological sort.

        Returns FQNs in execution order (leaves first).
        If strict=True, raises CycleDetectedError on cycles.
        If strict=False, cycles are broken by removing back-edges (best effort).
        """
        in_degree: Dict[str, int] = {fqn: len(deps) for fqn, deps in self._adj.items()}
        queue: deque[str] = deque(fqn for fqn, deg in in_degree.items() if deg == 0)
        order: List[str] = []

        while queue:
            node = queue.popleft()
            order.append(node)
            for caller in list(self._radj.get(node, set())):
                in_degree[caller] -= 1
                if in_degree[caller] == 0:
                    queue.append(caller)

        if len(order) < len(self._nodes):
            # Cycle exists — collect remaining nodes
            remaining = [n for n in self._nodes if n not in order]
            cycle = self._find_cycle(remaining[0]) if remaining else []
            if strict:
                raise CycleDetectedError(cycle)
            logger.warning(
                "dependency_cycle_detected",
                cycle=cycle,
                remaining_count=len(remaining),
            )
            # Append remaining nodes in arbitrary order (best effort)
            order.extend(remaining)

        return order

    def detect_cycles(self) -> List[List[str]]:
        """Return all cycles (as node lists) using DFS."""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        cycles: List[List[str]] = []

        def dfs(node: str, path: List[str]) -> None:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            for child in self._adj.get(node, set()):
                if child not in visited:
                    dfs(child, path)
                elif child in rec_stack:
                    # Found cycle — extract it
                    cycle_start = path.index(child)
                    cycles.append(path[cycle_start:] + [child])
            path.pop()
            rec_stack.discard(node)

        for node in list(self._nodes):
            if node not in visited:
                dfs(node, [])
        return cycles

    def assign_topo_levels(self) -> Dict[str, int]:
        """
        Assign an integer level to each node.
        Level 0 = leaves (no outgoing calls).
        Level N = depends only on nodes at levels < N.
        """
        # BFS from leaves upward
        levels: Dict[str, int] = {}
        out_degree = {fqn: len(self._adj[fqn]) for fqn in self._nodes}
        queue: deque[str] = deque(fqn for fqn, deg in out_degree.items() if deg == 0)
        for node in queue:
            levels[node] = 0

        while queue:
            node = queue.popleft()
            for caller in self._radj.get(node, set()):
                proposed = levels[node] + 1
                if caller not in levels or levels[caller] < proposed:
                    levels[caller] = proposed
                    queue.append(caller)

        # Any nodes not reached (isolated or in a cycle) get level 0
        for fqn in self._nodes:
            if fqn not in levels:
                levels[fqn] = 0

        self._levels = levels
        return levels

    def get_execution_batches(self) -> Dict[int, List[str]]:
        """
        Group nodes by topo_level. Nodes in the same batch can run in parallel.
        Returns {level: [fqn, ...]} sorted by level ascending (0 = first to run).
        """
        if not self._levels:
            self.assign_topo_levels()
        batches: Dict[int, List[str]] = defaultdict(list)
        for fqn, lvl in self._levels.items():
            batches[lvl].append(fqn)
        return dict(sorted(batches.items()))

    def get_execution_order(self) -> List[str]:
        """Flat list of FQNs in dependency-safe execution order (leaves first)."""
        return self.topological_sort()

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_lineage_rows(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Produce rows ready to INSERT into migration_lineage.
        Uses fqn strings as temporary IDs; the repository substitutes real UUIDs.
        """
        if not self._levels:
            self.assign_topo_levels()
        rows: List[Dict[str, Any]] = []
        for parent_fqn, children in self._adj.items():
            parent_node = self._nodes.get(parent_fqn)
            for child_fqn in children:
                child_node = self._nodes.get(child_fqn)
                if not child_node:
                    continue
                rows.append({
                    "job_id": job_id,
                    "parent_fqn": parent_fqn,
                    "child_fqn": child_fqn,
                    "reference_type": "CALLS",
                    "is_cross_schema": parent_node.schema != child_node.schema if parent_node else False,
                    "topo_level": self._levels.get(parent_fqn, 0),
                })
        return rows

    def to_dict(self) -> Dict[str, Any]:
        """Serialise full graph for API response and frontend consumption."""
        if not self._levels:
            self.assign_topo_levels()
        nodes = []
        for fqn, proc in self._nodes.items():
            nodes.append({
                "id": fqn,
                "schema": proc.schema,
                "name": proc.name,
                "object_type": proc.object_type,
                "db_platform": proc.db_platform,
                "is_encrypted": proc.is_encrypted,
                "topo_level": self._levels.get(fqn, 0),
                "param_count": len(proc.parameters),
                "char_count": proc.char_count,
            })
        edges = []
        for parent_fqn, children in self._adj.items():
            for child_fqn in children:
                edges.append({"source": parent_fqn, "target": child_fqn, "type": "CALLS"})
        batches = self.get_execution_batches()
        return {
            "nodes": nodes,
            "edges": edges,
            "execution_order": self.get_execution_order(),
            "execution_batches": {str(lvl): fqns for lvl, fqns in batches.items()},
            "node_count": len(nodes),
            "edge_count": len(edges),
            "max_depth": max(self._levels.values(), default=0),
            "has_cycles": bool(self.detect_cycles()),
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _find_cycle(self, start: str) -> List[str]:
        """DFS to find and return one cycle starting from `start`."""
        visited: Set[str] = set()
        path: List[str] = []

        def dfs(node: str) -> Optional[List[str]]:
            if node in visited:
                return None
            if node in set(path):
                idx = path.index(node)
                return path[idx:] + [node]
            path.append(node)
            visited.add(node)
            for child in self._adj.get(node, set()):
                result = dfs(child)
                if result:
                    return result
            path.pop()
            return None

        return dfs(start) or [start]

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, fqn: str) -> bool:
        return fqn in self._nodes
