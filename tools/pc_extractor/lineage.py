"""
Field-level lineage traversal for PowerCenter mappings.

Walks the CONNECTOR graph backwards from each target field to its source(s),
resolving expressions, lookups, and pass-throughs at each transformation hop.

Algorithm summary
-----------------
For each target field T in a mapping:
  1. Find the CONNECTOR where TOINSTANCE=<target> and TOFIELD=T.
  2. The FROMINSTANCE/FROMFIELD on that connector is the immediate upstream node.
  3. If the upstream is a Source Definition → record SourceRef and stop.
  4. Otherwise look up the transformation definition and the specific port:
     a. Collect the port's expression string.
     b. Find which INPUT (or INPUT/OUTPUT) ports are *referenced* in that expression
        via word-boundary regex — these are the logical data dependencies.
     c. For each referenced input port, follow the connector INTO that port
        (i.e. recurse with to_instance=<same trf>, to_field=<input port>).
     d. If no input ports are found (empty expression, constant like SYSDATE,
        or OUTPUT-only port with no inbound connections) fall back to directly
        re-tracing the upstream node: _trace_field(from_inst, from_fld) which
        follows the connector INTO that port — handles SQ OUTPUT-only ports.
  5. Special cases:
     - Expression OUTPUT port: capture expression, detect :LKP. unconnected lookups.
     - Lookup OUTPUT (return) port: annotate LookupRef, then trace the INPUT port.
     - Router OUTPUT port: use REF_FIELD attribute to find the corresponding INPUT.
     - Normalizer OUTPUT port: use REF_SOURCE_FIELD to find the INPUT group.
     - Union/Custom: use FIELDDEPENDENCY elements.

HIPAA note: expression content is never logged at INFO level.
"""
from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Tuple

from .models import (
    ConnectorDef, FieldLineage, FolderDef, LineageNode, LookupRef,
    MappingDef, MappingLineage, PortDef, SourceRef, TransformationDef,
    TransformationType,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Regex for unconnected lookup calls embedded in expression strings:  :LKP.LookupName(args)
_UNCONNECTED_LKP_RE = re.compile(r":LKP\.([_A-Za-z0-9]+)\(([^)]*)\)", re.IGNORECASE)

_MAX_DEPTH = 40

# Port type substrings that indicate an input-capable port
_INPUT_MARKERS = ("INPUT",)  # matches INPUT, INPUT/OUTPUT, INPUT/OUTPUT/MASTER, MASTER INPUT, DETAIL INPUT


# ---------------------------------------------------------------------------
# Internal traversal context
# ---------------------------------------------------------------------------

@dataclass
class _Ctx:
    mapping: MappingDef
    folder_sources: Dict        # name → SourceDef
    folder_targets: Dict        # name → TargetDef
    conn_index: Dict[Tuple[str, str], ConnectorDef] = field(default_factory=dict)
    inst_type_by_name: Dict[str, str] = field(default_factory=dict)
    trf_by_name: Dict[str, TransformationDef] = field(default_factory=dict)
    found_lookups: Dict[str, LookupRef] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for c in self.mapping.connectors:
            self.conn_index[(c.to_instance, c.to_field)] = c
        for inst in self.mapping.instances:
            self.inst_type_by_name[inst.name] = inst.transformation_type
        for trf in self.mapping.transformations:
            self.trf_by_name[trf.name] = trf


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def trace_mapping(
    mapping: MappingDef,
    folder_sources: Optional[Dict] = None,
    folder_targets: Optional[Dict] = None,
) -> MappingLineage:
    """
    Trace field-level lineage for every target field in *mapping*.

    Parameters
    ----------
    mapping:
        A parsed MappingDef (from pc_extractor.xml_parser).
    folder_sources:
        Dict[name, SourceDef] from the same folder — used to enrich source
        field type information.  Pass None to skip enrichment.
    folder_targets:
        Dict[name, TargetDef] from the same folder — used to enumerate target
        fields when the mapping itself doesn't list them.

    Returns
    -------
    MappingLineage with one FieldLineage per target field.
    """
    ctx = _Ctx(
        mapping=mapping,
        folder_sources=folder_sources or {},
        folder_targets=folder_targets or {},
    )
    result = MappingLineage(mapping_name=mapping.name, folder=mapping.folder)

    target_instances = [i for i in mapping.instances if i.transformation_type == "Target Definition"]

    for tgt_inst in target_instances:
        tgt_trf_name = tgt_inst.transformation_name
        tgt_def = ctx.folder_targets.get(tgt_trf_name)

        if tgt_def:
            target_fields = [f.name for f in tgt_def.fields]
        else:
            # Derive from connectors flowing into this instance
            target_fields = sorted({
                c.to_field for c in mapping.connectors
                if c.to_instance == tgt_inst.name
            })

        for tgt_field in target_fields:
            ctx.found_lookups.clear()
            chain: List[LineageNode] = []
            sources = _trace_field(ctx, tgt_inst.name, tgt_field, 0, frozenset(), chain)

            # Pick the first non-trivial expression from the chain as the summary
            expr = _summarise_expression(chain, tgt_field)

            result.fields.append(FieldLineage(
                target_table=tgt_trf_name,
                target_field=tgt_field,
                sources=sources,
                chain=chain,
                expression=expr,
                has_unconnected_lookup=any(not v.is_connected for v in ctx.found_lookups.values()),
                lookups=list(ctx.found_lookups.values()),
            ))

    return result


# ---------------------------------------------------------------------------
# Core traversal
# ---------------------------------------------------------------------------

def _trace_field(
    ctx: _Ctx,
    to_instance: str,
    to_field: str,
    depth: int,
    visited: FrozenSet[Tuple[str, str]],
    chain: List[LineageNode],
) -> List[SourceRef]:
    """
    Recursively trace one field backward through the connector graph.
    Appends LineageNode entries to *chain* as we walk upstream.
    Returns the list of ultimate SourceRef objects found.
    """
    if depth > _MAX_DEPTH:
        log.warning("Max traversal depth exceeded at %s.%s", to_instance, to_field)
        return []

    key = (to_instance, to_field)
    if key in visited:
        log.debug("Cycle detected at %s.%s — stopping", to_instance, to_field)
        return []
    visited = visited | {key}

    conn = ctx.conn_index.get(key)
    if conn is None:
        return []  # no upstream connector — constant, unconnected port

    from_inst = conn.from_instance
    from_fld = conn.from_field
    from_type = ctx.inst_type_by_name.get(from_inst, "")

    # ---- Terminal: Source Definition ----------------------------------------
    if from_type == "Source Definition":
        ftype = ""
        src_def = ctx.folder_sources.get(from_inst)
        if src_def:
            src_port = next((f for f in src_def.fields if f.name == from_fld), None)
            if src_port:
                ftype = src_port.datatype
        chain.append(LineageNode(instance=from_inst, field=from_fld, transform_type="Source Definition"))
        return [SourceRef(table=from_inst, field=from_fld, field_type=ftype)]

    # ---- Resolve transformation ----------------------------------------------
    trf = ctx.trf_by_name.get(from_inst)
    if trf is None:
        log.debug("No inline transformation definition for instance %s (reusable?)", from_inst)
        return []

    port = next((p for p in trf.ports if p.name == from_fld), None)
    port_type = port.port_type.upper() if port else ""
    expression = (port.expression if port else "") or ""

    node = LineageNode(
        instance=from_inst,
        field=from_fld,
        transform_type=trf.type.value,
        expression=expression,
    )
    chain.append(node)

    # ---- Dispatch by transformation type ------------------------------------

    if trf.type == TransformationType.LOOKUP:
        return _handle_lookup(ctx, trf, from_inst, from_fld, port_type, node, depth, visited, chain)

    if trf.type == TransformationType.ROUTER:
        return _handle_router(ctx, trf, from_inst, from_fld, port, depth, visited, chain)

    if trf.type == TransformationType.NORMALIZER:
        return _handle_normalizer(ctx, trf, from_inst, from_fld, port, depth, visited, chain)

    if trf.type in (TransformationType.UNION, TransformationType.CUSTOM):
        return _handle_union(ctx, trf, from_inst, from_fld, depth, visited, chain)

    # Detect unconnected lookups in expression strings (Expression, Aggregator, Filter, etc.)
    if expression:
        _detect_unconnected_lookups(ctx, expression, node)

    # Generic: find referenced input ports, recurse through connectors into each
    return _handle_generic(ctx, trf, from_inst, from_fld, expression, depth, visited, chain)


# ---------------------------------------------------------------------------
# Transformation-specific handlers
# ---------------------------------------------------------------------------

def _handle_lookup(
    ctx: _Ctx,
    trf: TransformationDef,
    inst: str,
    fld: str,
    port_type: str,
    node: LineageNode,
    depth: int,
    visited: FrozenSet,
    chain: List[LineageNode],
) -> List[SourceRef]:
    """
    Connected Lookup:
    - OUTPUT port = a return field.  Annotate with lookup metadata, then trace
      the INPUT port(s) that feed the lookup condition.
    - INPUT/OUTPUT port = pass-through (the same field is also an input).
    """
    lkp_table = trf.attributes.get("Lookup Table Name") or trf.attributes.get("Lookup table name") or ""
    lkp_cond = trf.lookup_condition
    sql_override = trf.attributes.get("Lookup Sql Override") or trf.attributes.get("Lookup SQL Override") or ""

    lkp_ref = LookupRef(
        lookup_name=inst,
        lookup_condition=lkp_cond,
        lookup_table=lkp_table or None,
        sql_override=sql_override or None,
        is_connected=True,
    )
    node.lookup_ref = lkp_ref
    if inst not in ctx.found_lookups:
        ctx.found_lookups[inst] = lkp_ref

    if "OUTPUT" in port_type and "INPUT" not in port_type:
        # Pure return port — follow the INPUT port(s) that drive the lookup
        input_ports = [p.name for p in trf.ports if "INPUT" in p.port_type.upper()]
        sources = []
        for inp in input_ports:
            sources.extend(_trace_field(ctx, inst, inp, depth + 1, visited, chain))
        return sources

    # INPUT/OUTPUT pass-through
    return _handle_generic(ctx, trf, inst, fld, fld, depth, visited, chain)


def _handle_router(
    ctx: _Ctx,
    trf: TransformationDef,
    inst: str,
    fld: str,
    port: Optional[PortDef],
    depth: int,
    visited: FrozenSet,
    chain: List[LineageNode],
) -> List[SourceRef]:
    """
    Router OUTPUT ports carry a REF_FIELD attribute naming the INPUT port they
    pass through.  Fall back to name matching if REF_FIELD is absent.
    """
    ref = port.ref_field if port else ""
    input_port = ref if ref else fld  # same name is typical when REF_FIELD absent
    return _trace_field(ctx, inst, input_port, depth + 1, visited, chain)


def _handle_normalizer(
    ctx: _Ctx,
    trf: TransformationDef,
    inst: str,
    fld: str,
    port: Optional[PortDef],
    depth: int,
    visited: FrozenSet,
    chain: List[LineageNode],
) -> List[SourceRef]:
    """
    Normalizer OUTPUT ports carry REF_SOURCE_FIELD that groups them with an INPUT.
    Find the INPUT port(s) sharing the same REF_SOURCE_FIELD value.
    """
    ref_src = port.ref_source_field if port else ""
    if ref_src:
        input_ports = [
            p.name for p in trf.ports
            if "INPUT" in p.port_type.upper() and p.ref_source_field == ref_src
        ]
        sources = []
        for inp in input_ports:
            sources.extend(_trace_field(ctx, inst, inp, depth + 1, visited, chain))
        if sources:
            return sources
    # Fallback: name match
    return _handle_generic(ctx, trf, inst, fld, fld, depth, visited, chain)


def _handle_union(
    ctx: _Ctx,
    trf: TransformationDef,
    inst: str,
    fld: str,
    depth: int,
    visited: FrozenSet,
    chain: List[LineageNode],
) -> List[SourceRef]:
    """Union/Custom: use FIELDDEPENDENCY to map output → input(s)."""
    input_ports = trf.field_dependencies.get(fld, [])
    if not input_ports:
        # No FIELDDEPENDENCY info — fall back to name matching
        input_ports = [fld]
    sources = []
    for inp in input_ports:
        sources.extend(_trace_field(ctx, inst, inp, depth + 1, visited, chain))
    return sources


def _handle_generic(
    ctx: _Ctx,
    trf: TransformationDef,
    inst: str,
    fld: str,
    expression: str,
    depth: int,
    visited: FrozenSet,
    chain: List[LineageNode],
) -> List[SourceRef]:
    """
    Generic handler used for: Expression, Filter, Source Qualifier, Joiner,
    Aggregator, Sorter, Rank, Update Strategy, Transaction Control, and any
    unrecognised type.

    Strategy:
    1. Collect all INPUT / INPUT-OUTPUT ports of this transformation.
    2. For each, test whether its name appears in `expression` as a whole word.
    3. If any match → recurse _trace_field for each matched input port.
    4. If none match → fall back to _trace_field(inst, fld) which follows the
       connector INTO (inst, fld), effectively skipping this hop.  This handles
       SQ OUTPUT-only ports and constants (SYSDATE etc.) correctly:
       - SQ OUTPUT port: recursing into (SQ, fld) finds the connector from
         Source → SQ and terminates at the Source Definition.
       - Pure constant (no upstream connector): returns [] immediately.
    """
    input_ports = _find_referenced_input_ports(trf, expression)

    if input_ports:
        sources = []
        for inp in input_ports:
            sources.extend(_trace_field(ctx, inst, inp, depth + 1, visited, chain))
        return sources

    # No input ports found in expression — follow connector directly into this port
    return _trace_field(ctx, inst, fld, depth + 1, visited, chain)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_referenced_input_ports(trf: TransformationDef, expression: str) -> List[str]:
    """
    Return the names of INPUT (or INPUT/OUTPUT) ports of *trf* whose names
    appear as whole words in *expression*.
    """
    if not expression:
        return []
    result = []
    for p in trf.ports:
        if not any(marker in p.port_type.upper() for marker in _INPUT_MARKERS):
            continue
        if re.search(r"\b" + re.escape(p.name) + r"\b", expression):
            result.append(p.name)
    return result


def _detect_unconnected_lookups(
    ctx: _Ctx, expression: str, node: LineageNode
) -> None:
    """
    Scan *expression* for :LKP.LookupName( patterns.  For each found, record
    a LookupRef (is_connected=False) in ctx.found_lookups and annotate the node.
    """
    for m in _UNCONNECTED_LKP_RE.finditer(expression):
        lkp_name = m.group(1)
        args_raw = m.group(2).strip()
        if lkp_name in ctx.found_lookups:
            continue

        # Look up the transformation for this lookup instance
        trf = ctx.trf_by_name.get(lkp_name)
        lkp_ref = LookupRef(
            lookup_name=lkp_name,
            lookup_condition=trf.lookup_condition if trf else None,
            lookup_table=(
                (trf.attributes.get("Lookup Table Name") or trf.attributes.get("Lookup table name"))
                if trf else None
            ),
            sql_override=(
                (trf.attributes.get("Lookup Sql Override") or trf.attributes.get("Lookup SQL Override"))
                if trf else None
            ),
            is_connected=False,
        )
        ctx.found_lookups[lkp_name] = lkp_ref
        node.lookup_ref = lkp_ref
        log.debug("Unconnected lookup detected: %s(args=%s)", lkp_name, args_raw)


def _summarise_expression(chain: List[LineageNode], target_field: str) -> str:
    """
    Pick the first non-trivial expression from the chain.
    Trivial = empty, or exactly the field name (pass-through).
    """
    for node in chain:
        expr = node.expression
        if expr and expr.strip() and expr.strip() != node.field:
            return expr.strip()
    return ""
