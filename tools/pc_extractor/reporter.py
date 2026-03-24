"""
Output writers for the migration manifest.

Produces:
  migration-manifest.json   — full structured manifest (all parseable data)
  migration-backlog.csv     — one row per mapping, sprint-planning friendly
  migration-summary.txt     — human-readable console summary
"""

import csv
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

from .models import MigrationManifest, TargetPlatform

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSON manifest
# ---------------------------------------------------------------------------

class _ManifestEncoder(json.JSONEncoder):
    """Handles dataclasses, Enums, and sets."""
    def default(self, obj: Any) -> Any:
        if hasattr(obj, "__dataclass_fields__"):
            return asdict(obj)
        if hasattr(obj, "value"):          # Enum
            return obj.value
        if isinstance(obj, set):
            return sorted(obj)
        return super().default(obj)


def write_manifest_json(manifest: MigrationManifest, output_dir: Path) -> Path:
    out = output_dir / "migration-manifest.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, cls=_ManifestEncoder, indent=2, ensure_ascii=False)
    log.info("Manifest written → %s", out)
    return out


# ---------------------------------------------------------------------------
# CSV sprint backlog
# ---------------------------------------------------------------------------

_CSV_COLUMNS = [
    "folder",
    "mapping_name",
    "is_valid",
    "complexity_score",
    "target_platform",
    "source_count",
    "target_count",
    "transformation_count",
    "connector_count",
    "variable_count",
    # Flags
    "flag_stored_proc",
    "flag_custom_transform",
    "flag_xml",
    "flag_sql_override",
    "flag_parameter_vars",
    "flag_multi_source",
    "flag_has_joiner",
    "flag_has_lookup",
    "flag_has_normalizer",
    "flag_update_strategy",
    # Source / target types (comma-separated)
    "source_db_types",
    "target_db_types",
    # Workflow membership
    "referenced_by_workflows",
    # Notes
    "complexity_reasons",
    "review_notes",
    # Sprint planning helper
    "sprint_estimate_days",
]


def _estimate_sprint_days(score: int, platform: TargetPlatform) -> float:
    """Very rough engineering effort estimate in person-days."""
    if platform == TargetPlatform.REVIEW:
        return round(score * 0.75, 1)   # 5-7.5 days for complex manual work
    if score <= 3:
        return 0.5
    if score <= 6:
        return 1.0
    return 2.0


def write_backlog_csv(manifest: MigrationManifest, output_dir: Path) -> Path:
    # Build workflow → [mapping names] reverse index
    workflow_refs: Dict[str, list] = {}   # mapping_name → [workflow_names]
    for folder in manifest.folders.values():
        for wf in folder.workflows.values():
            for mref in wf.mapping_refs:
                workflow_refs.setdefault(mref, []).append(wf.name)

    out = output_dir / "migration-backlog.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
        writer.writeheader()

        for folder in manifest.folders.values():
            for mapping in folder.mappings.values():
                f = mapping.flags
                platform = mapping.target_platform or TargetPlatform.GLUE
                score = mapping.complexity_score or 1

                # Gather source/target db types from folder definitions
                src_types = set()
                for sname in mapping.sources:
                    s = folder.sources.get(sname)
                    if s:
                        src_types.add(s.db_type.upper())

                tgt_types = set()
                for tname in mapping.targets:
                    t = folder.targets.get(tname)
                    if t:
                        tgt_types.add(t.db_type.upper())

                writer.writerow({
                    "folder":               mapping.folder,
                    "mapping_name":         mapping.name,
                    "is_valid":             mapping.is_valid,
                    "complexity_score":     score,
                    "target_platform":      platform.value,
                    "source_count":         len(mapping.sources),
                    "target_count":         len(mapping.targets),
                    "transformation_count": len(mapping.transformations),
                    "connector_count":      len(mapping.connectors),
                    "variable_count":       len(mapping.variables),
                    "flag_stored_proc":     f.has_stored_proc,
                    "flag_custom_transform":f.has_custom_transform,
                    "flag_xml":             f.has_xml,
                    "flag_sql_override":    f.has_sql_override,
                    "flag_parameter_vars":  f.has_parameter_vars,
                    "flag_multi_source":    f.multi_source,
                    "flag_has_joiner":      f.has_joiner,
                    "flag_has_lookup":      f.has_lookup,
                    "flag_has_normalizer":  f.has_normalizer,
                    "flag_update_strategy": f.has_update_strategy,
                    "source_db_types":      "|".join(sorted(src_types)),
                    "target_db_types":      "|".join(sorted(tgt_types)),
                    "referenced_by_workflows": "|".join(
                        workflow_refs.get(mapping.name, [])
                    ),
                    "complexity_reasons":   " // ".join(mapping.complexity_reasons),
                    "review_notes":         " // ".join(mapping.review_notes),
                    "sprint_estimate_days": _estimate_sprint_days(score, platform),
                })

    log.info("Backlog CSV written → %s", out)
    return out


# ---------------------------------------------------------------------------
# Console summary (rich or plain)
# ---------------------------------------------------------------------------

def _plain_summary(manifest: MigrationManifest) -> str:
    s = manifest.summary
    total_effort = 0.0
    for folder in manifest.folders.values():
        for m in folder.mappings.values():
            platform = m.target_platform or TargetPlatform.GLUE
            total_effort += _estimate_sprint_days(m.complexity_score or 1, platform)

    lines = [
        "",
        "=" * 62,
        "  MIGRATION MANIFEST SUMMARY",
        "=" * 62,
        f"  Repository:        {manifest.repository_name}",
        f"  Extracted at:      {manifest.extracted_at}",
        f"  Source files:      {len(manifest.source_files)}",
        "",
        "  INVENTORY",
        f"    Folders:         {s.total_folders}",
        f"    Sources:         {s.total_sources}",
        f"    Targets:         {s.total_targets}",
        f"    Mappings:        {s.total_mappings}  "
        f"(valid: {s.mappings_valid}  invalid: {s.mappings_invalid})",
        f"    Workflows:       {s.total_workflows}",
        "",
        "  SOURCE DB TYPES:   " + (", ".join(s.source_db_types) or "none detected"),
        "  TARGET DB TYPES:   " + (", ".join(s.target_db_types) or "none detected"),
        "",
        "  COMPLEXITY DISTRIBUTION",
        f"    Score 1-3  (simple):         {s.score_1_3:>5}  mappings",
        f"    Score 4-6  (moderate):       {s.score_4_6:>5}  mappings",
        f"    Score 7-8  (complex):        {s.score_7_8:>5}  mappings",
        f"    Score 9-10 (critical):       {s.score_9_10:>5}  mappings",
        "",
        "  PLATFORM ROUTING",
        f"    AWS Glue:                    {s.routed_glue:>5}  mappings",
        f"    Databricks:                  {s.routed_databricks:>5}  mappings",
        f"    Manual Review Required:      {s.routed_review:>5}  mappings",
        "",
        "  FLAGS (mappings affected)",
        f"    Stored procedure calls:      {s.flagged_stored_proc:>5}",
        f"    Custom/Java transforms:      {s.flagged_custom_transform:>5}",
        f"    XML transformations:         {s.flagged_xml:>5}",
        f"    SQL overrides:               {s.flagged_sql_override:>5}",
        f"    Parameter file usage:        {s.flagged_parameter_vars:>5}",
        "",
        f"  ESTIMATED EFFORT:  {total_effort:.0f} person-days  "
        f"({total_effort / 5:.0f} person-weeks)",
        "=" * 62,
        "",
    ]
    return "\n".join(lines)


def print_summary(manifest: MigrationManifest) -> None:
    try:
        from rich.console import Console
        from rich.table import Table
        from rich import box

        s = manifest.summary
        console = Console()
        console.print()
        console.rule("[bold cyan]MIGRATION MANIFEST SUMMARY[/bold cyan]")
        console.print(f"  Repository:  [bold]{manifest.repository_name}[/bold]")
        console.print(f"  Extracted:   {manifest.extracted_at}")
        console.print()

        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold magenta")
        t.add_column("Category", style="cyan")
        t.add_column("Detail")
        t.add_column("Count", justify="right")

        t.add_row("Inventory", "Folders",   str(s.total_folders))
        t.add_row("",          "Sources",   str(s.total_sources))
        t.add_row("",          "Targets",   str(s.total_targets))
        t.add_row("",          "Mappings",  f"{s.total_mappings} (invalid: {s.mappings_invalid})")
        t.add_row("",          "Workflows", str(s.total_workflows))
        t.add_row("Routing",   "Glue",       str(s.routed_glue))
        t.add_row("",          "Databricks", str(s.routed_databricks))
        t.add_row("",          "[red]Review[/red]", f"[red]{s.routed_review}[/red]")
        t.add_row("Complexity","Score 1-3",  str(s.score_1_3))
        t.add_row("",          "Score 4-6",  str(s.score_4_6))
        t.add_row("",          "Score 7-8",  str(s.score_7_8))
        t.add_row("",          "[red]Score 9-10[/red]", f"[red]{s.score_9_10}[/red]")
        t.add_row("Flags",     "Stored procs",        str(s.flagged_stored_proc))
        t.add_row("",          "Custom/Java",          str(s.flagged_custom_transform))
        t.add_row("",          "XML transforms",       str(s.flagged_xml))
        t.add_row("",          "SQL overrides",        str(s.flagged_sql_override))
        t.add_row("",          "Parameter files",      str(s.flagged_parameter_vars))

        console.print(t)
    except ImportError:
        print(_plain_summary(manifest))


def write_summary_txt(manifest: MigrationManifest, output_dir: Path) -> Path:
    out = output_dir / "migration-summary.txt"
    out.write_text(_plain_summary(manifest), encoding="utf-8")
    log.info("Summary written → %s", out)
    return out
