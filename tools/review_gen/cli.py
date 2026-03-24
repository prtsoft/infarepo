"""
review-gen CLI

Commands:
  generate-all   Generate review guides for all REVIEW-routed mappings
  generate       Generate a review guide for a single mapping
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

import click

from . import __version__
from .generator import generate_all, generate_single


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        level=level,
        stream=sys.stderr,
    )


@click.group()
@click.version_option(__version__)
def cli():
    """
    review-gen — Generate markdown review guides for REVIEW-routed mappings.

    For each mapping flagged as REVIEW (too complex for automatic code generation),
    produces a structured markdown guide with transformation inventory, stored
    procedure list, complexity reasons, and blank migration/test sections for
    the engineer to complete.
    """


@cli.command("generate-all")
@click.argument("manifest_json", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output-dir", "-o", default="output",
    type=click.Path(path_type=Path), show_default=True,
)
@click.option(
    "--folder", "-f", "folder_filter", multiple=True,
    help="Only generate for these folder names.",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def generate_all_cmd(manifest_json, output_dir, folder_filter, verbose):
    """
    Generate review guides for all REVIEW-routed mappings in the manifest.

    Output: <output_dir>/review_guides/<FOLDER>/<MAPPING>.md

    Example:

      review-gen generate-all output/manifest.json --output-dir output/
    """
    _setup_logging(verbose)

    click.echo(f"  Loading manifest: {manifest_json}")
    with open(manifest_json, encoding="utf-8") as fh:
        data = json.load(fh)
    manifest = _load_manifest(data)

    click.echo(f"  Generating review guides into: {output_dir}")
    click.echo()

    report = generate_all(
        manifest,
        output_dir=output_dir,
        folder_filter=list(folder_filter) if folder_filter else None,
    )

    click.echo(
        f"  Generated: {report.generated}  "
        f"Skipped: {report.skipped}  "
        f"Errors: {report.errors}  "
        f"Total: {report.total}"
    )
    for r in report.results:
        if not r.skipped and not r.error:
            click.echo(f"  [OK]  {r.output_path}")
        elif r.error:
            click.echo(f"  [ERROR] {r.folder}/{r.mapping} — {r.error}", err=True)

    if report.errors:
        sys.exit(1)


@cli.command("generate")
@click.argument("manifest_json", type=click.Path(exists=True, path_type=Path))
@click.argument("folder")
@click.argument("mapping")
@click.option(
    "--output-dir", "-o", default="output",
    type=click.Path(path_type=Path), show_default=True,
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def generate_cmd(manifest_json, folder, mapping, output_dir, verbose):
    """
    Generate a review guide for a single mapping.

    Example:

      review-gen generate manifest.json SALES_MART M_COMPLEX_MAPPING
    """
    _setup_logging(verbose)

    with open(manifest_json, encoding="utf-8") as fh:
        data = json.load(fh)
    manifest = _load_manifest(data)

    result = generate_single(manifest, folder, mapping, output_dir)
    if result.error:
        click.echo(f"  [ERROR] {result.error}", err=True)
        sys.exit(1)
    click.echo(f"  [OK]  {result.output_path}")


# ---------------------------------------------------------------------------
# Manifest loader (shared with other generators)
# ---------------------------------------------------------------------------

def _load_manifest(data: dict):
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent.parent))
    try:
        from glue_gen.cli import _load_manifest as _lm
        return _lm(data)
    except Exception:
        # Fallback minimal loader
        from pc_extractor.models import MigrationManifest, FolderDef, MappingDef, MappingFlags, TargetPlatform, ExtractionSummary
        def _mk_mapping(d):
            flags_d = d.get("flags", {})
            m = MappingDef(
                name=d["name"], folder=d["folder"],
                description=d.get("description", ""),
                is_valid=d.get("is_valid", True),
                sources=d.get("sources", []),
                targets=d.get("targets", []),
                flags=MappingFlags(
                    source_db_types=flags_d.get("source_db_types", []),
                    target_db_types=flags_d.get("target_db_types", []),
                ),
                complexity_score=d.get("complexity_score"),
                complexity_reasons=d.get("complexity_reasons", []),
                review_notes=d.get("review_notes", []),
            )
            tp = d.get("target_platform")
            m.target_platform = TargetPlatform(tp) if tp else None
            return m
        summary_d = data.get("summary", {})
        s_fields = ExtractionSummary.__dataclass_fields__
        summary = ExtractionSummary(**{k: summary_d.get(k, v.default if v.default is not v.default_factory else [])  # type: ignore
                                        for k, v in s_fields.items()})
        manifest = MigrationManifest(
            extracted_at=data.get("extracted_at", ""),
            source_files=data.get("source_files", []),
            repository_name=data.get("repository_name", ""),
            summary=summary,
        )
        for fname, fd in data.get("folders", {}).items():
            folder_obj = FolderDef(name=fd["name"], description=fd.get("description", ""))
            folder_obj.mappings = {k: _mk_mapping(v) for k, v in fd.get("mappings", {}).items()}
            manifest.folders[fname] = folder_obj
        return manifest


def main():
    cli()


if __name__ == "__main__":
    main()
