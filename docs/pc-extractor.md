# pc-extractor

Parses Informatica PowerCenter XML exports into a normalized migration manifest, scores each mapping for complexity, routes it to a target platform (Glue, Databricks, or REVIEW), and writes the planning artifacts your team uses for sprint backlog creation.

## Installation

```bash
cd tools/
pip install -e .
```

This installs the `pc-extractor` command. All other tools in this repo currently need to be run as modules (`python -m glue_gen.cli ...`); see [TODOS.md](../TODOS.md) for the packaging gap.

## Prerequisites

Export your PowerCenter objects to XML using one of:

- **Designer** → Repository → Export Objects → XML
- **Repository Manager** → right-click folder → Export
- **pmrep** command: `pmrep objectexport -f <folder> -o export.xml`

Each export file is a `POWERMART` XML document containing one or more `FOLDER` elements with `SOURCE`, `TARGET`, `MAPPING`, and `WORKFLOW` children.

## Commands

### `extract` — parse exports and produce planning artifacts

```
pc-extractor extract <inputs...> [OPTIONS]
```

**Arguments**

| Argument | Description |
|---|---|
| `inputs` | One or more `.xml` files or directories. Directories are searched recursively for `*.xml`. |

**Options**

| Option | Default | Description |
|---|---|---|
| `--output-dir`, `-o` | `.` | Directory to write output files into. Created if it doesn't exist. |
| `--skip-scoring` | false | Skip complexity scoring and platform routing. |
| `--folder-filter` | (all) | Repeatable. Only process the named folder(s). |
| `--verbose`, `-v` | false | Debug-level logging to stderr. |

**Output files**

| File | Description |
|---|---|
| `migration-manifest.json` | Full normalized model — all sources, targets, mappings, workflows, scores, platform routing. Consumed by `glue-gen` and `databricks-gen`. |
| `migration-backlog.csv` | One row per mapping with score, platform, sprint estimate, flags, and workflow references. Import into Jira/Azure Boards. |
| `migration-summary.txt` | Human-readable summary: counts, score distribution, platform routing split, flag totals. |

**Examples**

```bash
# Single file
pc-extractor extract exports/SALES_MART.xml -o output/

# All XML files in a directory
pc-extractor extract exports/ -o output/

# Multiple files, filter to two folders only
pc-extractor extract exports/*.xml -o output/ \
  --folder-filter SALES_MART \
  --folder-filter PATIENT_EXTRACT
```

---

### `summary` — print summary of an existing manifest

```
pc-extractor summary <manifest_file> [OPTIONS]
```

Re-prints the extraction summary without re-parsing. Useful when checking a manifest produced in a previous run.

```bash
pc-extractor summary output/migration-manifest.json
```

---

### `validate-xml` — check XML for parse errors

```
pc-extractor validate-xml <inputs...>
```

Checks that XML files are well-formed and parse without error, without performing a full extraction. Useful as a pre-flight check before a full run.

```bash
pc-extractor validate-xml exports/
```

---

### `ls-mappings` — quick mapping list

```
pc-extractor ls-mappings <manifest_file> [OPTIONS]
```

Prints all mappings with their score and platform in a tabular format. Supports filtering so you can quickly find, e.g., all REVIEW-flagged mappings.

**Options**

| Option | Description |
|---|---|
| `--platform` | Filter by platform: `GLUE`, `DATABRICKS`, or `REVIEW`. |
| `--min-score` | Only show mappings at or above this complexity score. |
| `--folder` | Only show mappings from this folder. |

```bash
# All REVIEW mappings
pc-extractor ls-mappings output/migration-manifest.json --platform REVIEW

# High-complexity mappings in one folder
pc-extractor ls-mappings output/migration-manifest.json \
  --folder SALES_MART --min-score 7
```

---

## Complexity scoring

Scores run from 1–10 and are computed once during `extract` (unless `--skip-scoring` is passed). Higher scores indicate more migration risk and effort.

| Rule | Points |
|---|---|
| Base | +1 |
| Joiner | +1 |
| Lookup | +1 |
| Router | +1 |
| Normalizer | +1 |
| Update Strategy | +1 |
| Stored Procedure | +3 |
| Java / External Procedure / Custom | +4 |
| XML Parser or Generator | +2 |
| Multi-source mapping | +1 |
| >10 transformations | +1 |
| >50 connectors | +1 |
| SQL override present | +1 |
| SQL override >500 chars | +1 |
| Mapping variables/parameters | +1 |
| >5 variables/parameters | +1 |
| `is_valid = NO` | +2 |

**Platform routing**

| Platform | Condition |
|---|---|
| `REVIEW` | Score ≥ 9, or has Custom/Java/External transform, or has Stored Procedure with score ≥ 7 |
| `DATABRICKS` | Any target DB type is Delta/S3/Parquet/ORC/Iceberg/Hive, or mapping name hints at lakehouse |
| `GLUE` | Everything else |

---

## Manifest JSON structure

The `migration-manifest.json` is the central artifact consumed by all downstream tools. Top-level structure:

```json
{
  "repository_name": "DEV_REPO",
  "extracted_at": "2026-03-23T...",
  "source_files": ["exports/SALES_MART.xml"],
  "summary": { ... },
  "folders": {
    "SALES_MART": {
      "sources": { "SRC_ORDERS": { ... } },
      "targets": { "TGT_FACT_ORDERS": { ... } },
      "mappings": {
        "M_LOAD_FACT_ORDERS": {
          "complexity_score": 3,
          "target_platform": "GLUE",
          "flags": { "has_joiner": false, ... },
          "connectors": [ ... ],
          "transformations": [ ... ]
        }
      },
      "workflows": { ... }
    }
  }
}
```

---

## Python API

All functionality is available without the CLI:

```python
from pc_extractor.xml_parser import parse_xml_files
from pc_extractor.scorer import score_all_mappings
from pathlib import Path

manifest = parse_xml_files([Path("exports/SALES_MART.xml")])
score_all_mappings(manifest)

folder = manifest.folders["SALES_MART"]
for name, mapping in folder.mappings.items():
    print(f"{name:40} score={mapping.complexity_score} platform={mapping.target_platform}")
```

### Field-level lineage

```python
from pc_extractor.lineage import trace_mapping

folder = manifest.folders["SALES_MART"]
mapping = folder.mappings["M_LOAD_FACT_ORDERS"]

lineage = trace_mapping(mapping, folder.sources, folder.targets)

for field in lineage.fields:
    sources = ", ".join(f"{s.table}.{s.field}" for s in field.sources)
    print(f"  {field.target_field:<25} ← {sources or '(derived)'}")
    if field.has_unconnected_lookup:
        print(f"    ⚠ unconnected lookup: {[l.lookup_name for l in field.lookups]}")
```

See [lineage.md](lineage.md) for full lineage API documentation.
