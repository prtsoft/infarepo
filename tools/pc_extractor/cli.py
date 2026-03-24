"""
pc-extractor CLI

Commands:
  extract       Parse PC XML exports → migration-manifest.json + backlog CSV
  summary       Print summary of an existing manifest
  validate-xml  Check XML exports for parse errors without full extraction
  ls-mappings   List all mappings with score/platform (quick view)
"""

import logging
import sys
from pathlib import Path

import click

from . import __version__
from .xml_parser import parse_xml_files
from .scorer import score_all_mappings
from .reporter import print_summary, write_manifest_json, write_backlog_csv, write_summary_txt


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        level=level,
        stream=sys.stderr,
    )


# ---------------------------------------------------------------------------
# CLI root
# ---------------------------------------------------------------------------

@click.group()
@click.version_option(__version__)
def cli():
    """
    pc-extractor — Informatica PowerCenter XML → Migration Manifest

    Parses PowerCenter XML exports and produces a normalized JSON manifest,
    sprint backlog CSV, and complexity summary.
    """


# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------

@cli.command()
@click.argument(
    "inputs", nargs=-1, required=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--output-dir", "-o",
    default=".",
    type=click.Path(path_type=Path),
    show_default=True,
    help="Directory to write output files into.",
)
@click.option(
    "--skip-scoring", is_flag=True, default=False,
    help="Skip complexity scoring and platform routing (faster for large repos).",
)
@click.option(
    "--folder-filter", "-f", "folder_filter",
    multiple=True,
    help="Only process folders matching these names (can specify multiple times).",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def extract(inputs, output_dir, skip_scoring, folder_filter, verbose):
    """
    Parse one or more PowerCenter XML export files.

    INPUTS can be individual .xml files or directories containing .xml files.

    Examples:

      pc-extractor extract exports/folder1.xml exports/folder2.xml

      pc-extractor extract exports/ --output-dir output/

      pc-extractor extract exports/ --folder-filter SALES --folder-filter HR
    """
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    # Expand directories to .xml files
    xml_paths: list[Path] = []
    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            found = sorted(p.rglob("*.xml"))
            if not found:
                click.echo(f"Warning: no .xml files found in {p}", err=True)
            xml_paths.extend(found)
        else:
            xml_paths.append(p)

    if not xml_paths:
        click.echo("Error: no XML files to process.", err=True)
        sys.exit(1)

    click.echo(f"  Found {len(xml_paths)} XML file(s) to process.")

    manifest = parse_xml_files(xml_paths)

    # Apply folder filter
    if folder_filter:
        allowed = set(folder_filter)
        removed = [k for k in manifest.folders if k not in allowed]
        for k in removed:
            del manifest.folders[k]
        click.echo(
            f"  Folder filter applied: kept {len(manifest.folders)} folder(s), "
            f"removed {len(removed)}."
        )

    if not skip_scoring:
        click.echo("  Scoring mappings...")
        score_all_mappings(manifest)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = write_manifest_json(manifest, output_dir)
    csv_path  = write_backlog_csv(manifest, output_dir)
    txt_path  = write_summary_txt(manifest, output_dir)

    print_summary(manifest)

    click.echo()
    click.echo("  Output files:")
    click.echo(f"    {json_path}")
    click.echo(f"    {csv_path}")
    click.echo(f"    {txt_path}")
    click.echo()


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------

@cli.command()
@click.argument(
    "manifest_file",
    type=click.Path(exists=True, path_type=Path),
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def summary(manifest_file, verbose):
    """
    Print a summary of an existing migration-manifest.json.

    Example:

      pc-extractor summary output/migration-manifest.json
    """
    _setup_logging(verbose)
    import json
    from .models import MigrationManifest, ExtractionSummary

    with open(manifest_file, encoding="utf-8") as fh:
        data = json.load(fh)

    # Reconstruct just enough for summary printing
    from dataclasses import fields
    summary_data = data.get("summary", {})
    s = ExtractionSummary(**{
        f.name: summary_data.get(f.name, f.default if f.default is not f.default_factory else [])  # type: ignore
        for f in fields(ExtractionSummary)
    })

    m = MigrationManifest(
        extracted_at=data.get("extracted_at", ""),
        source_files=data.get("source_files", []),
        repository_name=data.get("repository_name", ""),
        summary=s,
    )
    print_summary(m)


# ---------------------------------------------------------------------------
# validate-xml
# ---------------------------------------------------------------------------

@cli.command("validate-xml")
@click.argument(
    "inputs", nargs=-1, required=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def validate_xml(inputs, verbose):
    """
    Validate PowerCenter XML files without full extraction.
    Reports parse errors and basic structural issues.

    Example:

      pc-extractor validate-xml exports/*.xml
    """
    _setup_logging(verbose)
    from lxml import etree

    xml_paths: list[Path] = []
    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            xml_paths.extend(sorted(p.rglob("*.xml")))
        else:
            xml_paths.append(p)

    errors = 0
    for path in xml_paths:
        try:
            tree = etree.parse(str(path))
            root = tree.getroot()
            if root.tag != "POWERMART":
                click.echo(f"  [WARN]  {path.name}  — root tag is '{root.tag}', expected POWERMART")
                errors += 1
            else:
                repo = root.find("REPOSITORY")
                folders = repo.findall("FOLDER") if repo is not None else []
                mappings = sum(len(f.findall("MAPPING")) for f in folders)
                workflows = sum(len(f.findall("WORKFLOW")) for f in folders)
                click.echo(
                    f"  [OK]    {path.name}  "
                    f"folders={len(folders)}  mappings={mappings}  workflows={workflows}"
                )
        except etree.XMLSyntaxError as exc:
            click.echo(f"  [ERROR] {path.name}  — {exc}")
            errors += 1

    click.echo()
    if errors:
        click.echo(f"  {errors} file(s) had errors.", err=True)
        sys.exit(1)
    else:
        click.echo(f"  All {len(xml_paths)} file(s) are valid.")


# ---------------------------------------------------------------------------
# ls-mappings
# ---------------------------------------------------------------------------

@cli.command("ls-mappings")
@click.argument(
    "manifest_file",
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--platform", "-p",
    type=click.Choice(["GLUE", "DATABRICKS", "REVIEW", "ALL"], case_sensitive=False),
    default="ALL",
    show_default=True,
    help="Filter by target platform.",
)
@click.option(
    "--min-score", type=int, default=1,
    help="Only show mappings with score >= this value.",
)
@click.option(
    "--folder", "-f", "folder_name", default=None,
    help="Filter to a specific folder.",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def ls_mappings(manifest_file, platform, min_score, folder_name, verbose):
    """
    List all mappings from a manifest with score, platform, and flags.

    Example:

      pc-extractor ls-mappings output/migration-manifest.json --platform REVIEW

      pc-extractor ls-mappings output/migration-manifest.json --min-score 7
    """
    _setup_logging(verbose)
    import json

    with open(manifest_file, encoding="utf-8") as fh:
        data = json.load(fh)

    rows = []
    for fname, folder_data in data.get("folders", {}).items():
        if folder_name and fname != folder_name:
            continue
        for mname, m in folder_data.get("mappings", {}).items():
            score    = m.get("complexity_score") or 0
            mplatform = m.get("target_platform") or "GLUE"
            if platform != "ALL" and mplatform != platform.upper():
                continue
            if score < min_score:
                continue
            flags = m.get("flags", {})
            rows.append((fname, mname, score, mplatform, flags))

    rows.sort(key=lambda r: (-r[2], r[0], r[1]))   # sort by score desc

    try:
        from rich.console import Console
        from rich.table import Table
        from rich import box

        console = Console()
        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold magenta")
        t.add_column("Folder",   style="cyan",  no_wrap=True)
        t.add_column("Mapping",  style="white", no_wrap=True)
        t.add_column("Score",    justify="right")
        t.add_column("Platform", justify="center")
        t.add_column("Flags",    style="yellow")

        platform_style = {
            "GLUE":        "green",
            "DATABRICKS":  "blue",
            "REVIEW":      "red bold",
        }

        for fname, mname, score, mplatform, flags in rows:
            flag_list = []
            if flags.get("has_stored_proc"):      flag_list.append("SPROC")
            if flags.get("has_custom_transform"): flag_list.append("JAVA/CUSTOM")
            if flags.get("has_xml"):              flag_list.append("XML")
            if flags.get("has_sql_override"):     flag_list.append("SQL-OVERRIDE")
            if flags.get("has_parameter_vars"):   flag_list.append("PARAMS")
            if flags.get("multi_source"):         flag_list.append("MULTI-SRC")

            style = platform_style.get(mplatform, "white")
            t.add_row(
                fname, mname,
                str(score),
                f"[{style}]{mplatform}[/{style}]",
                " ".join(flag_list),
            )

        console.print(t)
        console.print(f"  {len(rows)} mapping(s) shown.")
    except ImportError:
        print(f"{'FOLDER':<30} {'MAPPING':<40} {'SCORE':>5}  {'PLATFORM':<12}  FLAGS")
        print("-" * 100)
        for fname, mname, score, mplatform, flags in rows:
            flag_list = []
            if flags.get("has_stored_proc"):      flag_list.append("SPROC")
            if flags.get("has_custom_transform"): flag_list.append("JAVA/CUSTOM")
            if flags.get("has_xml"):              flag_list.append("XML")
            if flags.get("has_sql_override"):     flag_list.append("SQL-OVERRIDE")
            if flags.get("has_parameter_vars"):   flag_list.append("PARAMS")
            print(f"{fname:<30} {mname:<40} {score:>5}  {mplatform:<12}  {' '.join(flag_list)}")
        print(f"\n{len(rows)} mapping(s) shown.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    cli()


if __name__ == "__main__":
    main()
