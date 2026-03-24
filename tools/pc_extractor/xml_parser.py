"""
PowerCenter XML export parser.

Handles the POWERMART DTD-based XML format produced by:
  - Repository Manager "Export Objects"
  - Designer "Export to XML"
  - pmrep ObjectExport command

Structure:
  POWERMART
    REPOSITORY
      FOLDER (one or more)
        SOURCE         (source definitions)
        TARGET         (target definitions)
        MAPPING        (mapping definitions)
          TRANSFORMATION
            TRANSFORMFIELD
            TABLEATTRIBUTE
            GROUPATTR       (ROUTER groups)
          CONNECTOR
          INSTANCE
          TARGETLOADORDER
          MAPPINGVARIABLE
        WORKFLOW
          SCHEDULERINFO
          TASKINSTANCE
          TASK (SESSION, COMMAND, etc.)
          LINK
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from lxml import etree

from .models import (
    ConnectorDef, ExtractionSummary, FieldDef, FolderDef, InstanceDef,
    MappingDef, MappingFlags, MappingVariableDef, MigrationManifest,
    PortDef, RouterGroupDef, SchedulerDef, SourceDef, TargetDef,
    TransformationDef, TransformationType, WorkflowDef, WorkflowLinkDef,
    WorkflowTaskDef,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _attr(element, name: str, default: str = "") -> str:
    """Get XML attribute, returning default if absent."""
    return element.get(name, default) or default


def _bool_attr(element, name: str) -> bool:
    return _attr(element, name, "NO").upper() in ("YES", "TRUE", "1")


def _int_attr(element, name: str, default: int = 0) -> int:
    try:
        return int(_attr(element, name, str(default)))
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# Source / Target parsers
# ---------------------------------------------------------------------------

def _parse_source(elem) -> SourceDef:
    db_type = _attr(elem, "DATABASETYPE")
    src = SourceDef(
        name=_attr(elem, "NAME"),
        db_type=db_type,
        db_name=_attr(elem, "DBDNAME"),
        owner=_attr(elem, "OWNERNAME"),
        description=_attr(elem, "DESCRIPTION"),
    )

    # Flat file specifics — PC stores these in TABLEATTRIBUTE children or
    # directly on the SOURCE element depending on version
    src.is_fixed_width = db_type.upper() in ("FLAT FILE",) and \
        _attr(elem, "FIXEDWIDTHTYPE", "0") != "0"
    src.delimiter = _attr(elem, "DELIMITEDTYPE", "")
    src.codepage = _attr(elem, "CODEPAGE", "")

    for f in elem.findall("SOURCEFIELD"):
        src.fields.append(FieldDef(
            name=_attr(f, "NAME"),
            datatype=_attr(f, "DATATYPE"),
            precision=_int_attr(f, "PRECISION"),
            scale=_int_attr(f, "SCALE"),
            length=_int_attr(f, "LENGTH"),
            nullable=_bool_attr(f, "NULLABLE"),
            key_type=_attr(f, "KEYTYPE", "NOT A KEY"),
        ))
    return src


def _parse_target(elem) -> TargetDef:
    tgt = TargetDef(
        name=_attr(elem, "NAME"),
        db_type=_attr(elem, "DATABASETYPE"),
        db_name=_attr(elem, "DBDNAME"),
        owner=_attr(elem, "OWNERNAME"),
        description=_attr(elem, "DESCRIPTION"),
    )
    for f in elem.findall("TARGETFIELD"):
        tgt.fields.append(FieldDef(
            name=_attr(f, "NAME"),
            datatype=_attr(f, "DATATYPE"),
            precision=_int_attr(f, "PRECISION"),
            scale=_int_attr(f, "SCALE"),
            nullable=_bool_attr(f, "NULLABLE"),
            key_type=_attr(f, "KEYTYPE", "NOT A KEY"),
        ))
    return tgt


# ---------------------------------------------------------------------------
# Transformation parser
# ---------------------------------------------------------------------------

# Attributes on TABLEATTRIBUTE that carry interesting SQL / config values
_SQ_SQL_ATTR       = "Sql Query"
_SQ_FILTER_ATTR    = "Source Filter"
_FILTER_COND_ATTR  = "Filter Condition"
_LOOKUP_COND_ATTR  = "Lookup Condition"
_JOINER_COND_ATTR  = "Join Condition"
_JOINER_TYPE_ATTR  = "Join Type"
_SP_NAME_ATTR      = "Stored Procedure Name"
_SP_NAME_ATTR2     = "Procedure Name"


def _parse_transformation(elem) -> TransformationDef:
    type_str = _attr(elem, "TYPE")
    t_type = TransformationType.from_str(type_str)

    t = TransformationDef(
        name=_attr(elem, "NAME"),
        type=t_type,
        reusable=_bool_attr(elem, "REUSABLE"),
        description=_attr(elem, "DESCRIPTION"),
    )

    # --- Ports (TRANSFORMFIELD) ---
    for pf in elem.findall("TRANSFORMFIELD"):
        t.ports.append(PortDef(
            name=_attr(pf, "NAME"),
            datatype=_attr(pf, "DATATYPE"),
            port_type=_attr(pf, "PORTTYPE"),
            expression=_attr(pf, "EXPRESSION"),
            default_value=_attr(pf, "DEFAULTVALUE"),
            precision=_int_attr(pf, "PRECISION"),
            scale=_int_attr(pf, "SCALE"),
        ))

    # --- Table Attributes (TABLEATTRIBUTE) ---
    for ta in elem.findall("TABLEATTRIBUTE"):
        k = _attr(ta, "NAME")
        v = _attr(ta, "VALUE")
        t.attributes[k] = v

    # --- Derived convenience fields from attributes ---
    t.sql_query       = t.attributes.get(_SQ_SQL_ATTR) or None
    t.filter_condition = (
        t.attributes.get(_FILTER_COND_ATTR)
        or t.attributes.get(_SQ_FILTER_ATTR)
        or None
    )
    t.lookup_condition  = t.attributes.get(_LOOKUP_COND_ATTR) or None
    t.join_condition    = t.attributes.get(_JOINER_COND_ATTR) or None
    t.join_type         = t.attributes.get(_JOINER_TYPE_ATTR) or None
    t.stored_proc_name  = (
        t.attributes.get(_SP_NAME_ATTR)
        or t.attributes.get(_SP_NAME_ATTR2)
        or None
    )

    # --- Router groups (GROUPATTR) ---
    for ga in elem.findall("GROUPATTR"):
        t.router_groups.append(RouterGroupDef(
            name=_attr(ga, "NAME"),
            condition=_attr(ga, "CONDITION"),
        ))

    return t


# ---------------------------------------------------------------------------
# Mapping parser
# ---------------------------------------------------------------------------

def _parse_mapping(elem, folder_name: str) -> MappingDef:
    m = MappingDef(
        name=_attr(elem, "NAME"),
        folder=folder_name,
        description=_attr(elem, "DESCRIPTION"),
        is_valid=_attr(elem, "ISVALID", "YES").upper() == "YES",
    )

    # --- Transformations ---
    for te in elem.findall("TRANSFORMATION"):
        m.transformations.append(_parse_transformation(te))

    # --- Instances (maps instance name → transformation name+type) ---
    for ie in elem.findall("INSTANCE"):
        inst_type = _attr(ie, "TYPE")  # SOURCE, TARGET, TRANSFORMATION
        m.instances.append(InstanceDef(
            name=_attr(ie, "NAME"),
            transformation_name=_attr(ie, "TRANSFORMATION"),
            transformation_type=_attr(ie, "TRANSFORMATIONTYPE") or inst_type,
            reusable=_bool_attr(ie, "REUSABLE"),
        ))
        if inst_type == "SOURCE":
            src_name = _attr(ie, "TRANSFORMATION")
            if src_name and src_name not in m.sources:
                m.sources.append(src_name)
        elif inst_type == "TARGET":
            tgt_name = _attr(ie, "TRANSFORMATION")
            if tgt_name and tgt_name not in m.targets:
                m.targets.append(tgt_name)

    # --- Connectors ---
    for ce in elem.findall("CONNECTOR"):
        m.connectors.append(ConnectorDef(
            from_instance=_attr(ce, "FROMINSTANCE"),
            from_field=_attr(ce, "FROMFIELD"),
            to_instance=_attr(ce, "TOINSTANCE"),
            to_field=_attr(ce, "TOFIELD"),
        ))

    # --- Mapping variables / parameters ---
    for ve in elem.findall("MAPPINGVARIABLE"):
        m.variables.append(MappingVariableDef(
            name=_attr(ve, "NAME"),
            datatype=_attr(ve, "DATATYPE"),
            is_param=_bool_attr(ve, "ISPARAM"),
            default_value=_attr(ve, "DEFAULTVALUE"),
            description=_attr(ve, "DESCRIPTION"),
        ))

    _compute_flags(m)
    return m


def _compute_flags(m: MappingDef) -> None:
    """Populate MappingFlags from parsed transformation data."""
    f = m.flags
    type_counts: Dict[str, int] = {}

    for t in m.transformations:
        type_counts[t.type.value] = type_counts.get(t.type.value, 0) + 1

        if t.type == TransformationType.STORED_PROCEDURE:
            f.has_stored_proc = True
        if t.type in (TransformationType.JAVA, TransformationType.EXTERNAL_PROCEDURE,
                      TransformationType.CUSTOM):
            f.has_custom_transform = True
        if t.type in (TransformationType.XML_PARSER, TransformationType.XML_GENERATOR):
            f.has_xml = True
        if t.type == TransformationType.NORMALIZER:
            f.has_normalizer = True
        if t.type == TransformationType.JOINER:
            f.has_joiner = True
        if t.type == TransformationType.LOOKUP:
            f.has_lookup = True
        if t.type == TransformationType.ROUTER:
            f.has_router = True
        if t.type == TransformationType.UPDATE_STRATEGY:
            f.has_update_strategy = True
        if t.type == TransformationType.SEQUENCE_GENERATOR:
            f.has_sequence_gen = True
        if t.type == TransformationType.SOURCE_QUALIFIER and t.sql_query:
            f.has_sql_override = True

    f.transformation_type_counts = type_counts
    f.multi_source = len(m.sources) > 1
    f.has_parameter_vars = any(v.is_param for v in m.variables)


# ---------------------------------------------------------------------------
# Workflow parser
# ---------------------------------------------------------------------------

def _parse_workflow(elem, folder_name: str) -> WorkflowDef:
    wf = WorkflowDef(
        name=_attr(elem, "NAME"),
        folder=folder_name,
        description=_attr(elem, "DESCRIPTION"),
        is_enabled=_attr(elem, "ISENABLED", "YES").upper() == "YES",
        is_valid=_attr(elem, "ISVALID", "YES").upper() == "YES",
        server_name=_attr(elem, "SERVERNAME"),
    )

    # --- Scheduler ---
    si = elem.find("SCHEDULERINFO")
    if si is not None:
        wf.scheduler = SchedulerDef(
            schedule_type=_attr(si, "SCHEDULETYPE"),
            start_time=_attr(si, "STARTTIME"),
            end_time=_attr(si, "ENDTIME"),
            raw_attributes={si.attrib[k]: v for k, v in si.attrib.items()},
        )

    # --- Tasks embedded in workflow (TASK elements) ---
    task_by_name: Dict[str, WorkflowTaskDef] = {}
    for te in elem.findall("TASK"):
        t = WorkflowTaskDef(
            name=_attr(te, "NAME"),
            task_type=_attr(te, "TYPE"),
            is_enabled=_attr(te, "ISENABLED", "YES").upper() == "YES",
            is_reusable=_bool_attr(te, "REUSABLE"),
            description=_attr(te, "DESCRIPTION"),
        )
        # SESSION tasks reference a mapping via SESSION_EXTENSION > MAPPING_REFERENCE
        # or more commonly via embedded config attributes
        se = te.find(".//SESSION_EXTENSION")
        if se is not None:
            mr = se.find("MAPPING_REFERENCE")
            if mr is not None:
                t.mapping_ref = _attr(mr, "MAPPING") or _attr(mr, "NAME")
        # Fallback: look for MAPPING attribute directly on task
        if not t.mapping_ref:
            t.mapping_ref = _attr(te, "MAPPING") or None

        wf.tasks.append(t)
        task_by_name[t.name] = t

    # --- Task instances (TASKINSTANCE = instance of a reusable task) ---
    for ti in elem.findall("TASKINSTANCE"):
        t = WorkflowTaskDef(
            name=_attr(ti, "NAME"),
            task_type=_attr(ti, "TASKTYPE"),
            is_enabled=True,
            description=_attr(ti, "DESCRIPTION"),
        )
        # Reusable session tasks reference mapping via TASKNAME (the reusable task)
        task_name_ref = _attr(ti, "TASKNAME")
        if task_name_ref and task_name_ref in task_by_name:
            t.mapping_ref = task_by_name[task_name_ref].mapping_ref
        wf.tasks.append(t)

    # --- Links ---
    for le in elem.findall("LINK"):
        wf.links.append(WorkflowLinkDef(
            from_task=_attr(le, "FROMTASK"),
            to_task=_attr(le, "TOTASK"),
            condition=_attr(le, "CONDITION"),
        ))

    # Collect unique mapping refs
    wf.mapping_refs = list({
        t.mapping_ref for t in wf.tasks
        if t.task_type.upper() == "SESSION" and t.mapping_ref
    })

    return wf


# ---------------------------------------------------------------------------
# Folder parser
# ---------------------------------------------------------------------------

def _parse_folder(elem) -> FolderDef:
    folder = FolderDef(
        name=_attr(elem, "NAME"),
        description=_attr(elem, "DESCRIPTION"),
    )

    for se in elem.findall("SOURCE"):
        src = _parse_source(se)
        folder.sources[src.name] = src

    for te in elem.findall("TARGET"):
        tgt = _parse_target(te)
        folder.targets[tgt.name] = tgt

    for me in elem.findall("MAPPING"):
        m = _parse_mapping(me, folder.name)
        folder.mappings[m.name] = m

    for we in elem.findall("WORKFLOW"):
        wf = _parse_workflow(we, folder.name)
        folder.workflows[wf.name] = wf

    return folder


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def parse_xml_file(path: Path) -> Tuple[str, Dict[str, FolderDef]]:
    """
    Parse a single PC XML export file.
    Returns (repository_name, {folder_name: FolderDef}).
    Raises ValueError on malformed XML.
    """
    log.info("Parsing %s", path)
    try:
        tree = etree.parse(str(path))
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"Malformed XML in {path}: {exc}") from exc

    root = tree.getroot()
    if root.tag != "POWERMART":
        raise ValueError(
            f"{path} does not appear to be a PowerCenter export "
            f"(root tag is '{root.tag}', expected 'POWERMART')"
        )

    repo_elem = root.find("REPOSITORY")
    if repo_elem is None:
        raise ValueError(f"{path}: no REPOSITORY element found")

    repo_name = _attr(repo_elem, "NAME", "UNKNOWN")
    folders: Dict[str, FolderDef] = {}

    for fe in repo_elem.findall("FOLDER"):
        folder = _parse_folder(fe)
        folders[folder.name] = folder
        log.info(
            "  Folder %-30s  sources=%-4d targets=%-4d mappings=%-4d workflows=%-4d",
            folder.name,
            len(folder.sources), len(folder.targets),
            len(folder.mappings), len(folder.workflows),
        )

    return repo_name, folders


def parse_xml_files(paths: List[Path]) -> MigrationManifest:
    """
    Parse one or more PC XML export files and merge into a single manifest.
    Files from the same repository/folder are merged; conflicts are logged.
    """
    from datetime import datetime, timezone

    manifest = MigrationManifest(
        extracted_at=datetime.now(timezone.utc).isoformat(),
        source_files=[str(p) for p in paths],
        repository_name="",
    )

    for path in paths:
        try:
            repo_name, folders = parse_xml_file(path)
        except ValueError as exc:
            log.error("Skipping %s: %s", path, exc)
            continue

        if not manifest.repository_name:
            manifest.repository_name = repo_name
        elif manifest.repository_name != repo_name:
            log.warning(
                "Repository name mismatch: have '%s', file '%s' says '%s'",
                manifest.repository_name, path.name, repo_name,
            )

        for fname, folder in folders.items():
            if fname in manifest.folders:
                existing = manifest.folders[fname]
                # Merge — later file wins on conflicts, warns on collision
                for k, v in folder.sources.items():
                    if k in existing.sources:
                        log.debug("Duplicate source '%s' in folder '%s' — overwriting", k, fname)
                    existing.sources[k] = v
                for k, v in folder.targets.items():
                    existing.targets[k] = v
                for k, v in folder.mappings.items():
                    if k in existing.mappings:
                        log.warning("Duplicate mapping '%s' in folder '%s'", k, fname)
                    existing.mappings[k] = v
                for k, v in folder.workflows.items():
                    existing.workflows[k] = v
            else:
                manifest.folders[fname] = folder

    _compute_summary(manifest)
    return manifest


def _compute_summary(manifest: MigrationManifest) -> None:
    s = manifest.summary
    s.total_folders   = len(manifest.folders)
    source_db_set: set = set()
    target_db_set: set = set()

    for folder in manifest.folders.values():
        s.total_sources   += len(folder.sources)
        s.total_targets   += len(folder.targets)
        s.total_mappings  += len(folder.mappings)
        s.total_workflows += len(folder.workflows)

        for src in folder.sources.values():
            source_db_set.add(src.db_type.upper())
        for tgt in folder.targets.values():
            target_db_set.add(tgt.db_type.upper())

        for m in folder.mappings.values():
            if m.is_valid:
                s.mappings_valid += 1
            else:
                s.mappings_invalid += 1

            if m.complexity_score is not None:
                if m.complexity_score <= 3:
                    s.score_1_3 += 1
                elif m.complexity_score <= 6:
                    s.score_4_6 += 1
                elif m.complexity_score <= 8:
                    s.score_7_8 += 1
                else:
                    s.score_9_10 += 1

            from .models import TargetPlatform
            if m.target_platform == TargetPlatform.GLUE:
                s.routed_glue += 1
            elif m.target_platform == TargetPlatform.DATABRICKS:
                s.routed_databricks += 1
            elif m.target_platform == TargetPlatform.REVIEW:
                s.routed_review += 1

            if m.flags.has_stored_proc:
                s.flagged_stored_proc += 1
            if m.flags.has_custom_transform:
                s.flagged_custom_transform += 1
            if m.flags.has_xml:
                s.flagged_xml += 1
            if m.flags.has_sql_override:
                s.flagged_sql_override += 1
            if m.flags.has_parameter_vars:
                s.flagged_parameter_vars += 1

    s.source_db_types = sorted(source_db_set)
    s.target_db_types = sorted(target_db_set)
