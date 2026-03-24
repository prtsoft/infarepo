"""
Pipeline graph — builds a DAG from PowerCenter mapping connectors and
provides topological ordering for code generation.

Nodes are PC *instance* names (not transformation names).
Edges are directed: FROMINSTANCE → TOINSTANCE.

Special node types:
  SOURCE  — instances of TYPE=SOURCE (no incoming edges in data flow)
  TARGET  — instances of TYPE=TARGET (no outgoing edges in data flow)
  XFORM   — intermediate transformation instances
"""

from __future__ import annotations
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pc_extractor.models import (
    ConnectorDef, InstanceDef, MappingDef, TransformationDef, TransformationType,
)


@dataclass
class PipelineNode:
    instance_name: str
    instance_type: str           # SOURCE, TARGET, TRANSFORMATION
    transformation_name: str
    transformation_type: str
    transformation_def: Optional[TransformationDef] = None
    # Ports flowing INTO this node from upstream (field name → source instance)
    incoming_fields: Dict[str, str] = field(default_factory=dict)


@dataclass
class PipelineEdge:
    from_node: str
    from_field: str
    to_node: str
    to_field: str


class PipelineGraph:
    """
    Directed acyclic graph of a PowerCenter mapping's data flow.

    Usage:
        graph = PipelineGraph.from_mapping(mapping, transformation_lookup)
        for node in graph.topological_order():
            ...
    """

    def __init__(self) -> None:
        self.nodes: Dict[str, PipelineNode] = {}
        self.edges: List[PipelineEdge] = []
        # Adjacency: node → list of downstream node names
        self._successors: Dict[str, List[str]] = defaultdict(list)
        # Adjacency: node → list of upstream node names
        self._predecessors: Dict[str, List[str]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_mapping(
        cls,
        mapping: MappingDef,
        transformation_lookup: Dict[str, TransformationDef],
    ) -> "PipelineGraph":
        g = cls()

        # Build nodes from instances
        for inst in mapping.instances:
            t_def = transformation_lookup.get(inst.transformation_name)
            g.nodes[inst.name] = PipelineNode(
                instance_name=inst.name,
                instance_type=inst.transformation_type,
                transformation_name=inst.transformation_name,
                transformation_type=inst.transformation_type,
                transformation_def=t_def,
            )

        # Build edges from connectors
        seen_edges: Set[tuple] = set()
        for conn in mapping.connectors:
            key = (conn.from_instance, conn.to_instance)
            g.edges.append(PipelineEdge(
                from_node=conn.from_instance,
                from_field=conn.from_field,
                to_node=conn.to_instance,
                to_field=conn.to_field,
            ))
            if key not in seen_edges:
                g._successors[conn.from_instance].append(conn.to_instance)
                g._predecessors[conn.to_instance].append(conn.from_instance)
                seen_edges.add(key)

        # Add any instances referenced by connectors but missing from instances list
        # (can happen with reusable transformations)
        for conn in mapping.connectors:
            for name in (conn.from_instance, conn.to_instance):
                if name not in g.nodes:
                    g.nodes[name] = PipelineNode(
                        instance_name=name,
                        instance_type="TRANSFORMATION",
                        transformation_name=name,
                        transformation_type="Unknown",
                    )

        return g

    # ------------------------------------------------------------------
    # Topology queries
    # ------------------------------------------------------------------

    def source_nodes(self) -> List[PipelineNode]:
        """Nodes with no predecessors (data entry points)."""
        return [
            n for name, n in self.nodes.items()
            if not self._predecessors.get(name)
        ]

    def target_nodes(self) -> List[PipelineNode]:
        """Nodes with no successors (data exit points)."""
        return [
            n for name, n in self.nodes.items()
            if not self._successors.get(name)
        ]

    def successors(self, node_name: str) -> List[PipelineNode]:
        return [self.nodes[n] for n in self._successors.get(node_name, []) if n in self.nodes]

    def predecessors(self, node_name: str) -> List[PipelineNode]:
        return [self.nodes[n] for n in self._predecessors.get(node_name, []) if n in self.nodes]

    def topological_order(self) -> List[PipelineNode]:
        """
        Kahn's algorithm — returns nodes in dependency order.
        Raises ValueError on cycle (should not happen in valid PC mappings).
        """
        in_degree: Dict[str, int] = {name: 0 for name in self.nodes}
        for name in self.nodes:
            for succ in self._successors.get(name, []):
                if succ in in_degree:
                    in_degree[succ] += 1

        queue = deque(name for name, deg in in_degree.items() if deg == 0)
        order: List[PipelineNode] = []

        while queue:
            name = queue.popleft()
            if name in self.nodes:
                order.append(self.nodes[name])
            for succ in self._successors.get(name, []):
                if succ not in in_degree:
                    continue
                in_degree[succ] -= 1
                if in_degree[succ] == 0:
                    queue.append(succ)

        if len(order) != len(self.nodes):
            # Cycle — fall back to insertion order (still generates valid code)
            return list(self.nodes.values())

        return order

    def fields_flowing_into(self, node_name: str) -> List[PipelineEdge]:
        """All connector edges where TOINSTANCE == node_name."""
        return [e for e in self.edges if e.to_node == node_name]

    def fields_flowing_out_of(self, node_name: str) -> List[PipelineEdge]:
        """All connector edges where FROMINSTANCE == node_name."""
        return [e for e in self.edges if e.from_node == node_name]

    def output_fields(self, node_name: str) -> List[str]:
        """Distinct field names flowing out of a node."""
        seen: Set[str] = set()
        result = []
        for e in self.fields_flowing_out_of(node_name):
            if e.from_field not in seen:
                seen.add(e.from_field)
                result.append(e.from_field)
        return result

    def input_fields(self, node_name: str) -> List[str]:
        """Distinct field names flowing into a node."""
        seen: Set[str] = set()
        result = []
        for e in self.fields_flowing_into(node_name):
            if e.to_field not in seen:
                seen.add(e.to_field)
                result.append(e.to_field)
        return result

    def primary_upstream(self, node_name: str) -> Optional[str]:
        """
        For nodes with exactly one upstream, returns that node name.
        For joiners with multiple upstreams, returns the detail (non-master) side.
        """
        preds = list(dict.fromkeys(self._predecessors.get(node_name, [])))
        if not preds:
            return None
        if len(preds) == 1:
            return preds[0]
        # Multiple — return first non-source-qualifier predecessor as "detail"
        for p in preds:
            n = self.nodes.get(p)
            if n and "source" not in n.transformation_type.lower():
                return p
        return preds[0]

    def all_upstream_names(self, node_name: str) -> List[str]:
        return list(dict.fromkeys(self._predecessors.get(node_name, [])))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def df_var(self, node_name: str) -> str:
        """Python variable name for the DataFrame produced by a node."""
        safe = node_name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
        return f"df_{safe}"

    def is_source_qualifier(self, node: PipelineNode) -> bool:
        return "source qualifier" in node.transformation_type.lower()

    def is_target(self, node: PipelineNode) -> bool:
        return node.instance_type in ("TARGET", "Target Definition") or \
               "target" in node.transformation_type.lower()

    def is_source_instance(self, node: PipelineNode) -> bool:
        return node.instance_type in ("SOURCE", "Source Definition") or \
               "source definition" in node.transformation_type.lower()
