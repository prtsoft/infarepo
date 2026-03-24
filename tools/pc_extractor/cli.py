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
# lineage
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("manifest_file", type=click.Path(exists=True, path_type=Path))
@click.argument("folder")
@click.argument("mapping")
@click.option(
    "--output-dir", "-o",
    default=None,
    type=click.Path(path_type=Path),
    help="Directory to write output file.  Defaults to current directory.",
)
@click.option(
    "--format", "-f", "output_format",
    type=click.Choice(["json", "csv", "excel", "text"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format: text (terminal), csv, excel (.xlsx), or json.",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def lineage(manifest_file, folder, mapping, output_dir, output_format, verbose):
    """
    Trace field-level lineage for a single mapping.

    MANIFEST_FILE  Path to migration-manifest.json from pc-extractor extract.
    FOLDER         Folder name containing the mapping.
    MAPPING        Mapping name.

    Examples:

      pc-extractor lineage output/migration-manifest.json SALES_MART M_LOAD_FACT_ORDERS

      pc-extractor lineage output/migration-manifest.json SALES_MART M_LOAD_FACT_ORDERS \\
        --format csv --output-dir output/lineage/
    """
    _setup_logging(verbose)
    import json as _json
    from .lineage import trace_mapping
    from .s2t_exporter import write_s2t_csv, write_s2t_excel

    with open(manifest_file, encoding="utf-8") as fh:
        data = _json.load(fh)

    folder_data = data.get("folders", {}).get(folder)
    if folder_data is None:
        click.echo(f"Error: folder '{folder}' not found in manifest.", err=True)
        sys.exit(1)

    mapping_data = folder_data.get("mappings", {}).get(mapping)
    if mapping_data is None:
        click.echo(f"Error: mapping '{mapping}' not found in folder '{folder}'.", err=True)
        sys.exit(1)

    # Re-parse the XML to get live objects (manifest JSON is reporting-only)
    xml_paths = [Path(p) for p in data.get("source_files", [])]
    manifest = parse_xml_files(xml_paths)
    score_all_mappings(manifest)

    folder_def = manifest.folders.get(folder)
    if folder_def is None:
        click.echo(f"Error: folder '{folder}' not found after re-parsing XML.", err=True)
        sys.exit(1)

    mapping_def = folder_def.mappings.get(mapping)
    if mapping_def is None:
        click.echo(f"Error: mapping '{mapping}' not found after re-parsing XML.", err=True)
        sys.exit(1)

    lin = trace_mapping(mapping_def, folder_def.sources, folder_def.targets)

    out_dir = Path(output_dir) if output_dir else Path(".")
    out_dir.mkdir(parents=True, exist_ok=True)

    fmt = output_format.lower()

    if fmt == "text":
        _print_lineage_text(lin)
    elif fmt == "csv":
        out_path = out_dir / f"s2t_{folder}_{mapping}.csv"
        write_s2t_csv(lin, out_path)
        click.echo(f"  Written: {out_path}")
    elif fmt == "excel":
        out_path = out_dir / f"s2t_{folder}_{mapping}.xlsx"
        write_s2t_excel(lin, out_path)
        click.echo(f"  Written: {out_path}")
    elif fmt == "json":
        import dataclasses
        out_path = out_dir / f"lineage_{folder}_{mapping}.json"
        out_path.write_text(
            _json.dumps(dataclasses.asdict(lin), indent=2, default=str),
            encoding="utf-8",
        )
        click.echo(f"  Written: {out_path}")


def _print_lineage_text(lin) -> None:
    """Print lineage to the terminal in a human-readable tabular format."""
    click.echo(f"\nLineage: {lin.folder}.{lin.mapping_name}\n")
    click.echo(f"  {'TARGET FIELD':<30}  SOURCE(S) / EXPRESSION")
    click.echo("  " + "-" * 78)
    for fl in lin.fields:
        if fl.sources:
            src_str = ", ".join(f"{s.table}.{s.field}" for s in fl.sources)
        else:
            src_str = fl.expression or "(unconnected)"
        lkp_str = f"  [lookups: {', '.join(l.lookup_name for l in fl.lookups)}]" if fl.lookups else ""
        click.echo(f"  {fl.target_field:<30}  ← {src_str}{lkp_str}")
    click.echo()


# ---------------------------------------------------------------------------
# lineage-all
# ---------------------------------------------------------------------------

@cli.command("lineage-all")
@click.argument("manifest_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output-dir", "-o",
    default="output/lineage",
    type=click.Path(path_type=Path),
    show_default=True,
    help="Directory to write S2T CSV files and the lineage index.",
)
@click.option(
    "--format", "-f", "output_format",
    type=click.Choice(["csv", "excel"], case_sensitive=False),
    default="csv",
    show_default=True,
    help="Output format for individual mapping files.",
)
@click.option(
    "--folder-filter", multiple=True,
    help="Only process these folder(s).",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def lineage_all(manifest_file, output_dir, output_format, folder_filter, verbose):
    """
    Trace lineage for ALL mappings and write one S2T file per mapping.

    Also writes lineage-index.json: a cross-mapping index mapping each
    source table name to the list of mappings that read from it.

    Example:

      pc-extractor lineage-all output/migration-manifest.json \\
        --output-dir output/lineage/ --format csv
    """
    _setup_logging(verbose)
    import json as _json
    from .lineage import trace_mapping
    from .s2t_exporter import write_s2t_csv, write_s2t_excel

    with open(manifest_file, encoding="utf-8") as fh:
        data = _json.load(fh)

    xml_paths = [Path(p) for p in data.get("source_files", [])]
    manifest = parse_xml_files(xml_paths)
    score_all_mappings(manifest)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    allowed_folders = set(folder_filter) if folder_filter else None
    total = 0
    errors = 0
    # cross-mapping index: {source_table: [mapping_full_name, ...]}
    source_index: dict[str, list[str]] = {}

    fmt = output_format.lower()
    ext = ".xlsx" if fmt == "excel" else ".csv"
    writer_fn = write_s2t_excel if fmt == "excel" else write_s2t_csv

    for folder_name, folder_def in manifest.folders.items():
        if allowed_folders and folder_name not in allowed_folders:
            continue
        folder_out = out_dir / folder_name
        folder_out.mkdir(parents=True, exist_ok=True)

        for mapping_name, mapping_def in folder_def.mappings.items():
            try:
                lin = trace_mapping(mapping_def, folder_def.sources, folder_def.targets)
                out_path = folder_out / f"s2t_{mapping_name}{ext}"
                writer_fn(lin, out_path)
                total += 1

                # Build cross-mapping index
                full_name = f"{folder_name}.{mapping_name}"
                for fl in lin.fields:
                    for src in fl.sources:
                        source_index.setdefault(src.table, [])
                        if full_name not in source_index[src.table]:
                            source_index[src.table].append(full_name)
            except Exception as exc:
                click.echo(f"  [ERROR] {folder_name}.{mapping_name}: {exc}", err=True)
                errors += 1

    # Write cross-mapping index
    index_path = out_dir / "lineage-index.json"
    index_path.write_text(
        _json.dumps({"source_table_to_mappings": source_index}, indent=2),
        encoding="utf-8",
    )

    click.echo(f"\n  Lineage complete: {total} mapping(s) processed, {errors} error(s).")
    click.echo(f"  S2T files written to: {out_dir}")
    click.echo(f"  Cross-mapping index:  {index_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    cli()


if __name__ == "__main__":
    main()
