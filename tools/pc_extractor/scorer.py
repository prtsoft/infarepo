"""
Complexity scorer and platform router for PowerCenter mappings.

Scoring rubric (cumulative, capped at 10):
  Base score = 1

  Transformation complexity:
    JOINER present                  +1
    LOOKUP present                  +1
    ROUTER present                  +1
    NORMALIZER present              +1
    UPDATE_STRATEGY present         +1
    SEQUENCE_GENERATOR present      +0  (trivial in Glue)
    STORED_PROCEDURE present        +3  (high risk — forces REVIEW consideration)
    JAVA / EXTERNAL_PROCEDURE /
      CUSTOM TRANSFORMATION         +4  (always forces REVIEW)
    XML_PARSER or XML_GENERATOR     +2
    HTTP_TRANSFORMATION             +2

  Data flow complexity:
    multi_source (>1 source inst)   +1
    >10 transformations total       +1
    >50 connectors                  +1

  SQL complexity:
    has_sql_override                +1
    sql_query length >500 chars     +1

  Parameter / variable complexity:
    has_parameter_vars              +1
    >5 mapping variables/params     +1

  Mapping validity:
    is_valid == False               +2  (broken mapping — risky)

Platform routing (applied after scoring):
  REVIEW  if score >= 9
           OR has_custom_transform
           OR has_stored_proc AND score >= 7
  DATABRICKS if any target db_type contains DELTA / S3 / PARQUET
              OR mapping name contains hints (extract, export, delta, lake)
  GLUE    everything else
"""

import re
import logging
from typing import List, Tuple

from .models import MappingDef, MigrationManifest, TargetPlatform, TransformationType

log = logging.getLogger(__name__)

# Keywords in mapping/folder name that suggest Databricks use
_DATABRICKS_NAME_HINTS = re.compile(
    r"(extract|export|delta|lake|lakehouse|parquet|orc|iceberg|feature|ml_|dbt)",
    re.IGNORECASE,
)

# Target DB types that indicate a lakehouse target
_LAKEHOUSE_DB_TYPES = {"DELTA", "S3", "PARQUET", "ORC", "ICEBERG", "HIVE"}


def _is_databricks_target(mapping: MappingDef, folder_targets) -> bool:
    """Return True if any target in this mapping looks like a lakehouse target."""
    for tgt_name in mapping.targets:
        tgt = folder_targets.get(tgt_name)
        if tgt and tgt.db_type.upper() in _LAKEHOUSE_DB_TYPES:
            return True
    return False


def score_mapping(
    mapping: MappingDef,
    folder_targets: dict,
) -> Tuple[int, List[str], TargetPlatform, List[str]]:
    """
    Compute (score, reasons, platform) for a single mapping.
    Does NOT mutate the mapping — caller applies the results.
    """
    score = 1
    reasons: List[str] = []
    f = mapping.flags

    # --- Transformation type complexity ---
    if f.has_joiner:
        score += 1
        reasons.append("Has JOINER transformation (+1)")
    if f.has_lookup:
        score += 1
        reasons.append("Has LOOKUP transformation (+1)")
    if f.has_router:
        score += 1
        reasons.append("Has ROUTER transformation (+1)")
    if f.has_normalizer:
        score += 1
        reasons.append("Has NORMALIZER transformation (+1)")
    if f.has_update_strategy:
        score += 1
        reasons.append("Has UPDATE_STRATEGY transformation (+1)")
    if f.has_stored_proc:
        score += 3
        reasons.append("Has STORED_PROCEDURE call (+3) — requires manual SQL review")
    if f.has_custom_transform:
        score += 4
        reasons.append(
            "Has JAVA / EXTERNAL_PROCEDURE / CUSTOM transformation (+4) — "
            "cannot be auto-converted"
        )
    if f.has_xml:
        score += 2
        reasons.append("Has XML_PARSER or XML_GENERATOR (+2)")
    # Check for HTTP transformation
    for t in mapping.transformations:
        if t.type == TransformationType.HTTP:
            score += 2
            reasons.append("Has HTTP_TRANSFORMATION (+2)")
            break

    # --- Data flow complexity ---
    if f.multi_source:
        score += 1
        reasons.append(f"Multiple source instances ({len(mapping.sources)}) (+1)")
    if len(mapping.transformations) > 10:
        score += 1
        reasons.append(f"High transformation count ({len(mapping.transformations)}) (+1)")
    if len(mapping.connectors) > 50:
        score += 1
        reasons.append(f"High connector count ({len(mapping.connectors)}) (+1)")

    # --- SQL complexity ---
    if f.has_sql_override:
        score += 1
        reasons.append("SOURCE_QUALIFIER has SQL override (+1)")
        # Extra point for long/complex SQL
        for t in mapping.transformations:
            if t.type == TransformationType.SOURCE_QUALIFIER and t.sql_query:
                if len(t.sql_query) > 500:
                    score += 1
                    reasons.append(
                        f"SQL override is long ({len(t.sql_query)} chars) — "
                        "likely complex subqueries or unions (+1)"
                    )
                break

    # --- Parameter / variable complexity ---
    if f.has_parameter_vars:
        score += 1
        reasons.append("Uses mapping parameters ($$PARAM) (+1)")
    if len(mapping.variables) > 5:
        score += 1
        reasons.append(
            f"High variable/parameter count ({len(mapping.variables)}) (+1)"
        )

    # --- Validity penalty ---
    if not mapping.is_valid:
        score += 2
        reasons.append("Mapping marked INVALID in repository (+2) — inspect before migrating")

    # Cap at 10
    score = min(score, 10)

    # --- Platform routing ---
    review_notes: List[str] = []
    platform = _route_platform(mapping, folder_targets, score, review_notes)

    return score, reasons, platform, review_notes


def _route_platform(
    mapping: MappingDef,
    folder_targets: dict,
    score: int,
    review_notes: List[str],
) -> TargetPlatform:
    f = mapping.flags

    # Hard REVIEW conditions
    if f.has_custom_transform:
        review_notes.append(
            "Java / External Procedure / Custom transformation cannot be auto-converted. "
            "Manually rewrite logic in PySpark or replace with equivalent AWS service."
        )
        return TargetPlatform.REVIEW

    if score >= 9:
        review_notes.append(
            f"Complexity score {score}/10 exceeds auto-conversion threshold. "
            "Assign to senior engineer for manual migration."
        )
        return TargetPlatform.REVIEW

    if f.has_stored_proc:
        review_notes.append(
            "Stored procedure call requires manual review. "
            "Inline the procedure logic as Glue/Spark SQL or replace with an "
            "equivalent AWS service (e.g., Lambda, RDS stored proc call via JDBC)."
        )
        return TargetPlatform.REVIEW

    # Databricks routing
    if _is_databricks_target(mapping, folder_targets):
        return TargetPlatform.DATABRICKS

    if _DATABRICKS_NAME_HINTS.search(mapping.name):
        review_notes.append(
            "Mapping name suggests extract/lakehouse pattern — routed to Databricks. "
            "Verify this is correct."
        )
        return TargetPlatform.DATABRICKS

    return TargetPlatform.GLUE


def score_all_mappings(manifest: MigrationManifest) -> None:
    """
    Score every mapping in the manifest in-place.
    Also re-computes the summary score distribution and routing counts.
    """
    # Reset counters
    s = manifest.summary
    s.score_1_3 = s.score_4_6 = s.score_7_8 = s.score_9_10 = 0
    s.routed_glue = s.routed_databricks = s.routed_review = 0

    for folder in manifest.folders.values():
        for mapping in folder.mappings.values():
            score, reasons, platform, review_notes = score_mapping(
                mapping, folder.targets
            )
            mapping.complexity_score  = score
            mapping.complexity_reasons = reasons
            mapping.target_platform   = platform
            mapping.review_notes      = review_notes

            if score <= 3:
                s.score_1_3 += 1
            elif score <= 6:
                s.score_4_6 += 1
            elif score <= 8:
                s.score_7_8 += 1
            else:
                s.score_9_10 += 1

            if platform == TargetPlatform.GLUE:
                s.routed_glue += 1
            elif platform == TargetPlatform.DATABRICKS:
                s.routed_databricks += 1
            elif platform == TargetPlatform.REVIEW:
                s.routed_review += 1

    log.info(
        "Scoring complete: GLUE=%d  DATABRICKS=%d  REVIEW=%d  "
        "(1-3: %d  4-6: %d  7-8: %d  9-10: %d)",
        s.routed_glue, s.routed_databricks, s.routed_review,
        s.score_1_3, s.score_4_6, s.score_7_8, s.score_9_10,
    )
