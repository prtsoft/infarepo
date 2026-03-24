"""
review_gen generator — Generate REVIEW guidance markdown for REVIEW-routed mappings.

For each REVIEW-routed mapping, writes review_guides/<FOLDER>/<MAPPING>.md with:
  - Mapping summary (score, reason for REVIEW)
  - Transformation inventory table
  - Stored procedure names
  - Complexity reasons
  - Blank "Migration Steps" and "Test Cases" sections for the engineer
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pc_extractor.models import MappingDef, MigrationManifest, TargetPlatform

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report types
# ---------------------------------------------------------------------------

@dataclass
class ReviewGuideResult:
    folder: str
    mapping: str
    output_path: Optional[str] = None
    skipped: bool = False
    skip_reason: str = ""
    error: Optional[str] = None


@dataclass
class ReviewGenerationReport:
    generated_at: str
    total: int = 0
    generated: int = 0
    skipped: int = 0
    errors: int = 0
    results: List[ReviewGuideResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Markdown builder
# ---------------------------------------------------------------------------

def _build_review_guide(mapping: MappingDef) -> str:
    """Build a review guide markdown string for a REVIEW-routed mapping."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    score = mapping.complexity_score or "?"
    reasons = mapping.complexity_reasons or []
    review_notes = mapping.review_notes or []

    # Transformation table
    trf_rows = []
    for trf in mapping.transformations:
        ttype = trf.type.value if trf.type else "UNKNOWN"
        ports = len(trf.ports)
        attrs = ", ".join(f"{k}={v}" for k, v in list((trf.attributes or {}).items())[:3])
        sp = f"`{trf.stored_proc_name}`" if trf.stored_proc_name else ""
        trf_rows.append(f"| `{trf.name}` | {ttype} | {ports} | {sp} | {attrs} |")

    trf_table = "\n".join(trf_rows) if trf_rows else "| _(no transformations parsed)_ | | | | |"

    # Stored procedures
    sp_names = sorted({
        trf.stored_proc_name
        for trf in mapping.transformations
        if trf.stored_proc_name
    })
    sp_section = ""
    if sp_names:
        sp_list = "\n".join(f"- `{sp}`" for sp in sp_names)
        sp_section = f"""
## Stored Procedures

The following stored procedures are referenced in this mapping and may need to be
migrated to Databricks SQL UDFs, PySpark UDFs, or called via JDBC passthrough:

{sp_list}

> **Action required:** For each procedure above, determine whether to:
> 1. Rewrite as a PySpark UDF or Databricks SQL function
> 2. Call via `spark.sql("CALL schema.proc_name(...)")` (Databricks Unity Catalog)
> 3. Use JDBC passthrough: `spark.read.jdbc(..., query="EXEC proc_name")`
"""

    # Complexity reasons
    if reasons:
        reason_list = "\n".join(f"- {r}" for r in reasons)
        complexity_section = f"""
## Why This Mapping Is Under Review

Complexity score: **{score}/10**

{reason_list}
"""
    else:
        complexity_section = f"""
## Why This Mapping Is Under Review

Complexity score: **{score}/10**

This mapping was routed to REVIEW because automatic code generation was not
confident it could handle all aspects of the transformation. Manual analysis required.
"""

    # Review notes
    notes_section = ""
    if review_notes:
        notes_list = "\n".join(f"- {n}" for n in review_notes)
        notes_section = f"""
## Automated Review Notes

The following issues or observations were raised during analysis:

{notes_list}
"""

    # Source/target lists
    sources = ", ".join(f"`{s}`" for s in mapping.sources) if mapping.sources else "_unknown_"
    targets = ", ".join(f"`{t}`" for t in mapping.targets) if mapping.targets else "_unknown_"

    # Connector summary
    conn_lines = []
    for conn in mapping.connectors:
        conn_lines.append(
            f"- `{conn.from_transformation}`.`{conn.from_field}` → "
            f"`{conn.to_transformation}`.`{conn.to_field}`"
        )
    conn_section = "\n".join(conn_lines[:20])
    if len(mapping.connectors) > 20:
        conn_section += f"\n- _(+{len(mapping.connectors) - 20} more connectors — see manifest)_"

    return f"""# Review Guide: {mapping.name}

**Folder:** {mapping.folder}
**Date:** {now}
**Status:** REVIEW REQUIRED
**Complexity Score:** {score}/10
{complexity_section}
---

## Mapping Summary

| Property | Value |
|---|---|
| Mapping Name | `{mapping.name}` |
| Folder | `{mapping.folder}` |
| Source(s) | {sources} |
| Target(s) | {targets} |
| Complexity Score | {score}/10 |
| Is Valid XML | {'Yes' if mapping.is_valid else 'No'} |

---

## Transformation Inventory

| Name | Type | Ports | Stored Proc | Key Attributes |
|---|---|---|---|---|
{trf_table}

{sp_section}
{notes_section}

---

## Key Data Flows

Top-level field-level connections (first 20):

{conn_section or "_(no connectors parsed)_"}

---

## Migration Steps

> **Instructions:** Complete the steps below before marking this mapping as done.

- [ ] Review source SQL queries and filters for SQL Server / Oracle compatibility
- [ ] Identify any on-premises file paths and migrate to S3 equivalents
- [ ] Resolve stored procedure dependencies (see section above)
- [ ] Write PySpark equivalent logic for each transformation
- [ ] Generate notebook using `databricks-gen generate` or `glue-gen generate`
- [ ] Review generated code and address any `# TODO` comments
- [ ] Run unit tests against a sample dataset
- [ ] Validate row counts using `validation-harness recon`

---

## Test Cases

> **Instructions:** Define test cases below before user acceptance testing.

| # | Input Scenario | Expected Output | Pass/Fail |
|---|---|---|---|
| 1 | Normal load (happy path) | All rows reconcile within 0.5% | |
| 2 | Empty source table | 0 rows written to target, no errors | |
| 3 | Null values in key columns | Handled per business rules | |
| 4 | _(add more rows as needed)_ | | |

---

## Sign-Off

| Role | Name | Date | Signature |
|---|---|---|---|
| Migration Engineer | | | |
| Data Steward | | | |
| QA Reviewer | | | |
"""


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def generate_all(
    manifest: MigrationManifest,
    output_dir: Path,
    folder_filter: Optional[List[str]] = None,
) -> ReviewGenerationReport:
    """Generate review guides for all REVIEW-routed mappings."""
    output_dir = Path(output_dir)
    report = ReviewGenerationReport(
        generated_at=datetime.now(timezone.utc).isoformat()
    )

    for folder_name, folder in manifest.folders.items():
        if folder_filter and folder_name not in folder_filter:
            continue

        for mapping in folder.mappings.values():
            report.total += 1

            if mapping.target_platform != TargetPlatform.REVIEW:
                result = ReviewGuideResult(
                    folder=folder_name, mapping=mapping.name,
                    skipped=True,
                    skip_reason=mapping.target_platform.value if mapping.target_platform else "not REVIEW",
                )
                report.skipped += 1
                report.results.append(result)
                continue

            try:
                guide_dir = output_dir / "review_guides" / folder_name
                guide_dir.mkdir(parents=True, exist_ok=True)
                guide_path = guide_dir / f"{mapping.name}.md"
                guide_path.write_text(_build_review_guide(mapping), encoding="utf-8")

                result = ReviewGuideResult(
                    folder=folder_name, mapping=mapping.name,
                    output_path=str(guide_path),
                )
                report.generated += 1
                log.info("  [REVIEW] %-50s", mapping.name)
            except Exception as exc:
                log.error("  [ERROR] %s / %s: %s", folder_name, mapping.name, exc)
                result = ReviewGuideResult(
                    folder=folder_name, mapping=mapping.name, error=str(exc)
                )
                report.errors += 1

            report.results.append(result)

    _write_report(report, output_dir)
    log.info(
        "Review guides: generated=%d  skipped=%d  errors=%d  total=%d",
        report.generated, report.skipped, report.errors, report.total,
    )
    return report


def generate_single(
    manifest: MigrationManifest,
    folder_name: str,
    mapping_name: str,
    output_dir: Path,
) -> ReviewGuideResult:
    """Generate a review guide for a single mapping."""
    folder = manifest.folders.get(folder_name)
    if not folder:
        raise ValueError(f"Folder '{folder_name}' not found")
    mapping = folder.mappings.get(mapping_name)
    if not mapping:
        raise ValueError(f"Mapping '{mapping_name}' not found in '{folder_name}'")

    guide_dir = Path(output_dir) / "review_guides" / folder_name
    guide_dir.mkdir(parents=True, exist_ok=True)
    guide_path = guide_dir / f"{mapping_name}.md"
    guide_path.write_text(_build_review_guide(mapping), encoding="utf-8")

    return ReviewGuideResult(
        folder=folder_name, mapping=mapping_name,
        output_path=str(guide_path),
    )


def _write_report(report: ReviewGenerationReport, output_dir: Path) -> None:
    path = output_dir / "review-generation-report.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(asdict(report), fh, indent=2)
    log.info("Review generation report → %s", path)
