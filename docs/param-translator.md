# param-translator

Parses Informatica PowerCenter `.prm` parameter files, classifies each parameter by type, normalizes values, diffs files across environments, and exports JSON artifacts consumed by `glue-gen` and `databricks-gen` for job parameterization.

## Background

PowerCenter parameter files (`.prm`) drive runtime behavior of workflows and sessions. They look like:

```ini
[SALES_MART.WF_SALES_MART_DAILY:s_M_LOAD_FACT_ORDERS]
$$START_DATE=2026-01-01
$$END_DATE=2026-03-31
$$SOURCE_SCHEMA=dbo
$$SQL_FILTER=ORDER_DATE >= '2026-01-01'

[SALES_MART.WF_SALES_MART_DAILY]
$$ENV=PROD
$$LOG_DIR=C:\InfaLogs\SALES_MART

[Global]
$$DB_USER=etl_svc_account
```

During migration these need to become:
- **Glue job arguments** (`--START_DATE`, `--SOURCE_SCHEMA`)
- **Databricks widgets** (`dbutils.widgets.text(...)`)
- **AWS SSM Parameter Store** values (for secrets like connection strings)
- **Terraform variables** (for non-secret config)

## Running

```bash
cd tools/
python -m param_translator.cli <command> [OPTIONS]
```

## Commands

### `parse` — parse `.prm` files and export JSON artifacts

```
python -m param_translator.cli parse <inputs...> [OPTIONS]
```

Parses one or more `.prm` files (or directories), classifies each parameter, normalizes values, and writes structured JSON output.

**Options**

| Option | Default | Description |
|---|---|---|
| `--output-dir`, `-o` | `output` | Directory to write output files. |
| `--verbose`, `-v` | false | Debug logging. |

**Output files**

| File | Description |
|---|---|
| `params-by-session.json` | Parameters grouped by `folder.workflow:session`. Used by glue-gen to inject job args. |
| `params-by-workflow.json` | Workflow-scope parameters. |
| `params-global.json` | Global parameters shared across all workflows. |
| `translation-report.json` | Per-parameter classification, normalized value, and any warnings needing manual review. |

```bash
python -m param_translator.cli parse params/ --output-dir output/params/
```

---

### `validate` — check `.prm` files for migration issues

```
python -m param_translator.cli validate <inputs...>
```

Runs without writing any files. Reports:

- Empty parameters that look required (`$$SQL_*`, `$$DATE_*`, `$$PATH_*`)
- On-premises file paths that need to be migrated to S3 (`C:\`, `\\server\`, `/data/`)
- HIPAA-sensitive parameter names (`PHI`, `PII`, `SSN`, `DOB`, `MRN`, `PATIENT`, etc.)
- Parameters classified as STRING that might actually be DATE or SQL (possible misclassification)

```bash
python -m param_translator.cli validate params/PATIENT_EXTRACT.prm
```

Sample output:

```
  Sev     File                  Section                           Param        Message
  ──────  ────────────────────  ────────────────────────────────  ───────────  ─────────────────────────────
  HIPAA   PATIENT_EXTRACT.prm   PATIENT_EXTRACT.WF:s_EXTRACT      $$DOB_COL    Param name suggests PHI/PII
  WARN    SALES_MART.prm        Global                            $$LOG_DIR    On-premises path — migrate to S3

  1 HIPAA flag(s) — review with your privacy officer before deploying.
```

---

### `diff` — compare two `.prm` files

```
python -m param_translator.cli diff <file_a> <file_b>
```

Compares two `.prm` files and shows what was added, removed, or changed. Typically used to compare DEV vs PROD parameter files before migrating.

```bash
python -m param_translator.cli diff \
  params/dev/SALES_MART.prm \
  params/prod/SALES_MART.prm
```

Sample output:

```
  A: SALES_MART.prm  B: SALES_MART.prm (prod)
  Added: 1  Removed: 0  Changed: 2

  ADDED (in B, not in A):
    + $$ALERT_EMAIL              = 'ops@company.com'  [STRING]

  CHANGED:
    ~ $$START_DATE
        A: '2026-01-01'
        B: '2025-01-01'
    ~ $$LOG_DIR
        A: 'C:\InfaLogs\dev'
        B: 'C:\InfaLogs\prod'
```

---

### `show` — inspect a single `.prm` file

```
python -m param_translator.cli show <prm_file> [OPTIONS]
```

Displays classified and normalized parameters from a single file. Useful for debugging classification results.

**Options**

| Option | Description |
|---|---|
| `--section`, `-s` | Show only a specific section key. |
| `--type-filter`, `-t` | Show only parameters of this type (STRING, DATE, SQL, PATH, INTEGER, EMPTY). |
| `--json-output` | Output raw JSON. |

```bash
# Show all parameters
python -m param_translator.cli show params/SALES_MART.prm

# Show only SQL parameters
python -m param_translator.cli show params/SALES_MART.prm --type-filter SQL

# Show raw JSON
python -m param_translator.cli show params/SALES_MART.prm --json-output
```

---

## Parameter type classification

The classifier assigns each parameter one of these types:

| Type | Description | Examples |
|---|---|---|
| `DATE` | A date or datetime string | `2026-01-01`, `01/01/2026 00:00:00` |
| `SQL` | A SQL statement or fragment | `SELECT * FROM ...`, `ORDER_DATE >= ...` |
| `PATH` | A file or directory path | `C:\InfaLogs\`, `s3://bucket/prefix/` |
| `INTEGER` | A numeric value | `100`, `0` |
| `BOOLEAN` | A true/false value | `YES`, `NO`, `TRUE`, `FALSE` |
| `STRING` | Everything else | `PROD`, `us-east-1`, `etl_user` |
| `EMPTY` | Empty value | `` |

Classification drives how values are handled in migration output:
- `DATE` → Glue job arg / Databricks widget with date validation hint
- `SQL` → Multi-line value, needs careful escaping
- `PATH` → Flag on-prem paths for S3 migration
- `EMPTY` → Flag as potentially missing required value

---

## Python API

```python
from param_translator.parser import parse_prm_file
from param_translator.classifier import classify_file
from param_translator.normalizer import normalize_file
from pathlib import Path

prm = parse_prm_file(Path("params/SALES_MART.prm"))
classify_file(prm)
normalize_file(prm)

# Access merged view (all sections flattened, session overrides workflow overrides global)
for name, param in prm.merged.items():
    print(f"  {name:<30} [{param.param_type}] = {param.normalized_value!r}")

# Access by section
for section in prm.sections:
    print(f"\n[{section.key}]  ({section.section_type})")
    for name, param in section.params.items():
        print(f"  {name} = {param.raw_value!r}  → {param.normalized_value!r}")
```
