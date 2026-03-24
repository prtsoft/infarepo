"""
Normalized data models for PowerCenter objects.
All PC-specific terminology is preserved in field names for traceability.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum


class TargetPlatform(str, Enum):
    GLUE = "GLUE"
    DATABRICKS = "DATABRICKS"
    REVIEW = "REVIEW"          # too complex for auto-conversion


class TransformationType(str, Enum):
    SOURCE_QUALIFIER    = "Source Qualifier"
    EXPRESSION          = "Expression"
    FILTER              = "Filter"
    AGGREGATOR          = "Aggregator"
    JOINER              = "Joiner"
    LOOKUP              = "Lookup Procedure"
    STORED_PROCEDURE    = "Stored Procedure"
    ROUTER              = "Router"
    UNION               = "Union"
    NORMALIZER          = "Normalizer"
    SEQUENCE_GENERATOR  = "Sequence Generator"
    SORTER              = "Sorter"
    RANK                = "Rank"
    JAVA                = "Java Transformation"
    EXTERNAL_PROCEDURE  = "External Procedure"
    HTTP                = "HTTP Transformation"
    XML_PARSER          = "XML Parser"
    XML_GENERATOR       = "XML Generator"
    MAPPLET             = "Mapplet"
    INPUT               = "Input Transformation"
    OUTPUT              = "Output Transformation"
    UPDATE_STRATEGY     = "Update Strategy"
    TRANSACTION_CONTROL = "Transaction Control"
    CUSTOM              = "Custom Transformation"
    UNKNOWN             = "Unknown"

    @classmethod
    def from_str(cls, value: str) -> "TransformationType":
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        return cls.UNKNOWN


@dataclass
class FieldDef:
    name: str
    datatype: str
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    key_type: str = "NOT A KEY"   # PRIMARY KEY, FOREIGN KEY, NOT A KEY
    length: int = 0


@dataclass
class PortDef:
    name: str
    datatype: str
    port_type: str = ""           # INPUT, OUTPUT, INPUT/OUTPUT, LOOKUP
    expression: str = ""
    default_value: str = ""
    precision: int = 0
    scale: int = 0
    ref_source_field: str = ""    # Normalizer: OUTPUT port's REF_SOURCE_FIELD group key
    ref_field: str = ""           # Router: OUTPUT port's corresponding INPUT port name
    expression_type: str = ""     # Aggregator: GROUPBY / GENERAL


@dataclass
class RouterGroupDef:
    """Represents a ROUTER transformation output group."""
    name: str
    condition: str


@dataclass
class TransformationDef:
    name: str
    type: TransformationType
    reusable: bool = False
    description: str = ""
    ports: List[PortDef] = field(default_factory=list)
    # Raw table attributes (k/v pairs from TABLEATTRIBUTE elements)
    attributes: Dict[str, str] = field(default_factory=dict)
    # Derived convenience fields
    sql_query: Optional[str] = None          # SOURCE_QUALIFIER sql override
    filter_condition: Optional[str] = None  # FILTER / SOURCE_QUALIFIER filter
    lookup_condition: Optional[str] = None  # LOOKUP condition
    stored_proc_name: Optional[str] = None  # STORED_PROCEDURE name
    router_groups: List[RouterGroupDef] = field(default_factory=list)
    join_condition: Optional[str] = None    # JOINER condition
    join_type: Optional[str] = None         # JOINER: Normal, Master Outer, Detail Outer, Full Outer
    field_dependencies: Dict[str, List[str]] = field(default_factory=dict)
    # {output_field: [input_fields]} — from FIELDDEPENDENCY elements (Union/Custom)


@dataclass
class ConnectorDef:
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str


@dataclass
class MappingVariableDef:
    name: str
    datatype: str
    is_param: bool = False        # ISPARAM="YES" = parameter, NO = variable
    default_value: str = ""
    description: str = ""


@dataclass
class InstanceDef:
    """An instance of a transformation or source/target within a mapping."""
    name: str
    transformation_name: str
    transformation_type: str
    reusable: bool = False


@dataclass
class SourceDef:
    name: str
    db_type: str                  # SQLSERVER, ORACLE, FLAT FILE, EXCEL, etc.
    db_name: str = ""
    owner: str = ""
    description: str = ""
    fields: List[FieldDef] = field(default_factory=list)
    # Flat file specific
    is_fixed_width: bool = False
    delimiter: str = ""
    codepage: str = ""


@dataclass
class TargetDef:
    name: str
    db_type: str
    db_name: str = ""
    owner: str = ""
    description: str = ""
    fields: List[FieldDef] = field(default_factory=list)


@dataclass
class MappingFlags:
    """Pre-computed flags used by the scorer and code generators."""
    has_stored_proc: bool = False
    has_parameter_vars: bool = False       # uses $$PARAM style variables
    has_sql_override: bool = False         # SOURCE_QUALIFIER with custom SQL
    has_custom_transform: bool = False     # Java, External Procedure, Custom
    has_xml: bool = False
    has_normalizer: bool = False
    has_joiner: bool = False
    has_lookup: bool = False
    has_router: bool = False
    has_update_strategy: bool = False
    has_sequence_gen: bool = False
    multi_source: bool = False             # more than one source instance
    source_db_types: List[str] = field(default_factory=list)
    target_db_types: List[str] = field(default_factory=list)
    transformation_type_counts: Dict[str, int] = field(default_factory=dict)


@dataclass
class MappingDef:
    name: str
    folder: str
    description: str = ""
    is_valid: bool = True
    sources: List[str] = field(default_factory=list)       # source instance names
    targets: List[str] = field(default_factory=list)       # target instance names
    instances: List[InstanceDef] = field(default_factory=list)
    transformations: List[TransformationDef] = field(default_factory=list)
    connectors: List[ConnectorDef] = field(default_factory=list)
    variables: List[MappingVariableDef] = field(default_factory=list)
    flags: MappingFlags = field(default_factory=MappingFlags)
    # Filled in by scorer
    complexity_score: Optional[int] = None
    complexity_reasons: List[str] = field(default_factory=list)
    target_platform: Optional[TargetPlatform] = None
    review_notes: List[str] = field(default_factory=list)


@dataclass
class WorkflowTaskDef:
    name: str
    task_type: str               # SESSION, COMMAND, DECISION, EMAIL, TIMER, EVENT-WAIT, WORKLET
    is_enabled: bool = True
    is_reusable: bool = False
    mapping_ref: Optional[str] = None   # populated for SESSION tasks
    description: str = ""
    command_script: str = ""            # COMMAND task: shell command / script line
    task_variables: List[str] = field(default_factory=list)  # $$VAR refs found in this task


@dataclass
class WorkflowLinkDef:
    from_task: str
    to_task: str
    condition: str = ""          # empty = unconditional


@dataclass
class SchedulerDef:
    schedule_type: str = ""      # ON_DEMAND, CONTINUOUS, CUSTOMIZED, RUN_ONCE
    start_time: str = ""
    end_time: str = ""
    raw_attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class WorkflowDef:
    name: str
    folder: str
    description: str = ""
    is_enabled: bool = True
    is_valid: bool = True
    server_name: str = ""
    scheduler: SchedulerDef = field(default_factory=SchedulerDef)
    tasks: List[WorkflowTaskDef] = field(default_factory=list)
    links: List[WorkflowLinkDef] = field(default_factory=list)
    # Names of mappings referenced via SESSION tasks
    mapping_refs: List[str] = field(default_factory=list)
    # Workflow-level parameters ($$VAR → default value)
    parameters: Dict[str, str] = field(default_factory=dict)
    # Quick-access flags for generation/scoring
    has_command_tasks: bool = False   # pre/post-session COMMAND tasks present
    has_event_wait: bool = False      # EVENT-WAIT tasks present (file-arrival pattern)


@dataclass
class FolderDef:
    name: str
    description: str = ""
    sources: Dict[str, SourceDef] = field(default_factory=dict)
    targets: Dict[str, TargetDef] = field(default_factory=dict)
    mappings: Dict[str, MappingDef] = field(default_factory=dict)
    workflows: Dict[str, WorkflowDef] = field(default_factory=dict)
    # Reusable transformation definitions (REUSABLE="YES") stored by name.
    # Populated during parsing so that inline mapping instances can resolve
    # their port definitions without requiring the full repository XML.
    reusable_transformations: Dict[str, "TransformationDef"] = field(default_factory=dict)


@dataclass
class ExtractionSummary:
    total_folders: int = 0
    total_sources: int = 0
    total_targets: int = 0
    total_mappings: int = 0
    total_workflows: int = 0
    mappings_valid: int = 0
    mappings_invalid: int = 0
    # Score distribution
    score_1_3: int = 0
    score_4_6: int = 0
    score_7_8: int = 0
    score_9_10: int = 0
    # Platform routing
    routed_glue: int = 0
    routed_databricks: int = 0
    routed_review: int = 0
    # Flag counts
    flagged_stored_proc: int = 0
    flagged_custom_transform: int = 0
    flagged_xml: int = 0
    flagged_sql_override: int = 0
    flagged_parameter_vars: int = 0
    source_db_types: List[str] = field(default_factory=list)
    target_db_types: List[str] = field(default_factory=list)


@dataclass
class MigrationManifest:
    extracted_at: str
    source_files: List[str]
    repository_name: str
    folders: Dict[str, FolderDef] = field(default_factory=dict)
    summary: ExtractionSummary = field(default_factory=ExtractionSummary)


# ---------------------------------------------------------------------------
# Field-level lineage models
# ---------------------------------------------------------------------------

@dataclass
class SourceRef:
    """An ultimate source field at the end of a lineage chain."""
    table: str        # source instance name (matches SourceDef.name)
    field: str        # source field name
    field_type: str = ""


@dataclass
class LookupRef:
    """A lookup transformation encountered during lineage traversal."""
    lookup_name: str
    lookup_condition: Optional[str] = None
    lookup_table: Optional[str] = None
    sql_override: Optional[str] = None
    is_connected: bool = True     # False = unconnected (:LKP.Name() in expression)


@dataclass
class LineageNode:
    """One hop in a field's lineage chain."""
    instance: str
    field: str
    transform_type: str
    expression: str = ""
    lookup_ref: Optional[LookupRef] = None


@dataclass
class FieldLineage:
    """Full lineage for a single target field."""
    target_table: str
    target_field: str
    sources: List[SourceRef] = field(default_factory=list)
    chain: List[LineageNode] = field(default_factory=list)
    expression: str = ""              # consolidated expression (first non-trivial one in chain)
    has_unconnected_lookup: bool = False
    lookups: List[LookupRef] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class MappingLineage:
    """All field lineage for a mapping."""
    mapping_name: str
    folder: str
    fields: List[FieldLineage] = field(default_factory=list)
