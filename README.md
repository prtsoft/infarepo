# PowerCenter → AWS Glue / Databricks Migration Toolchain

End-to-end toolchain for migrating Informatica PowerCenter 10.5 workloads to AWS Glue (PySpark) and Databricks (Delta Lake). Covers extraction, scoring, code generation, parameter translation, and post-migration validation.

**Project**: PC 10.5 → AWS Glue / Databricks
**Target deadline**: Q3 2027
**Compliance**: HIPAA (PHI-bearing mappings handled with data-value suppression throughout)

---

## End-to-end workflow

```
intake.py
    │  migration-intake.json
    ▼
pc-extractor extract
    │  migration-manifest.json   (scored, platform-routed)
    ▼
param-translator parse
    │  params-by-session.json
    ├──────────────────────────────┐
    ▼                              ▼
glue-gen generate-all      databricks-gen generate-all
    │  glue_jobs/*.py              │  notebooks/*.py
    │  terraform/*.tf              │  terraform/*.tf
    ▼                              ▼
workflow-gen generate-all   review-gen generate-all
    │  step-functions/             │  review_guides/*.md
    │  airflow/                    │  (manual migration guides)
    ▼
validation-harness validate
    │  validation-report.json
    │  validation-summary.txt
    ▼
validation-harness sign-off   →  sign-off report (HTML)
```

Or run the whole sequence in one command:

```bash
python run_pipeline.py --intake migration-intake.json
```

---

## Prerequisites

- Python 3.10+
- For SQL Server sources: [ODBC Driver 17 or 18 for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- For Databricks targets: `databricks-sql-connector` (installed via extras below)
- AWS CLI configured (for Terraform / SSM steps)

---

## Installation

```bash
cd tools/

# Core tools only
pip install -e .

# Everything a migration engineer needs
pip install -e ".[all,dev]"

# Specific extras
pip install -e ".[sql-server]"   # SQL Server JDBC driver (pyodbc)
pip install -e ".[databricks]"   # Databricks SQL connector
pip install -e ".[spark]"        # PySpark for local script testing
pip install -e ".[aws]"          # boto3 for SSM / S3
pip install -e ".[excel]"        # openpyxl for S2T Excel export
pip install -e ".[reports]"      # jinja2 for HTML sign-off reports
```

After installing, all tools are available as commands:

```
pc-extractor        validate, extract, summarize, lineage
glue-gen            generate-all, generate, preview, report
databricks-gen      generate-all, generate, report
param-translator    parse, validate, diff, show, export-ssm
validation-harness  validate, recon, diff-schema, config-gen, sign-off, test-connection
workflow-gen        generate-all
review-gen          generate-all
```

---

## Quick start

### 1. Run the intake questionnaire (once per project)

```bash
python intake.py
# → writes migration-intake.json
```

### 2. Extract and score PowerCenter mappings

```bash
pc-extractor extract exports/*.xml --output-dir output/
# → writes output/migration-manifest.json
# → writes output/migration-backlog.csv
# → writes output/migration-summary.txt
```

### 3. Review what will be generated

```bash
pc-extractor ls-mappings output/migration-manifest.json --platform GLUE
pc-extractor ls-mappings output/migration-manifest.json --platform DATABRICKS
pc-extractor ls-mappings output/migration-manifest.json --platform REVIEW
```

### 4. Translate parameter files

```bash
param-translator parse params/ --output-dir output/params/
# → writes output/params/params-by-session.json
```

### 5. Generate Glue jobs and Databricks notebooks

```bash
glue-gen generate-all output/migration-manifest.json \
  --output-dir generated/ \
  --params-dir output/params/

databricks-gen generate-all output/migration-manifest.json \
  --output-dir generated/ \
  --params-dir output/params/
```

### 6. Generate workflow orchestration

```bash
workflow-gen generate-all output/migration-manifest.json \
  --target step-functions \
  --output-dir generated/
```

### 7. Validate post-migration

```bash
export SOURCE_PASSWORD="..."
export DATABRICKS_TOKEN="..."

validation-harness validate config/sales_mart_validation.yaml \
  --output-dir output/validation/
```

### 8. Generate lineage / S2T documentation

```bash
pc-extractor lineage-all output/migration-manifest.json \
  --output-dir output/lineage/
```

---

## Tool documentation

| Tool | Docs |
|---|---|
| `pc-extractor` | [docs/pc-extractor.md](docs/pc-extractor.md) |
| `glue-gen` | [docs/glue-gen.md](docs/glue-gen.md) |
| `databricks-gen` | [docs/databricks-gen.md](docs/databricks-gen.md) |
| `param-translator` | [docs/param-translator.md](docs/param-translator.md) |
| `validation-harness` | [docs/validation-harness.md](docs/validation-harness.md) |
| Lineage Python API | [docs/lineage.md](docs/lineage.md) |

---

## Running tests

```bash
cd tools/
pip install -e ".[dev]"
pytest tests/ -v
```

---

## HIPAA notice

This toolchain processes mappings that may contain PHI column names (PATIENT_ID, DOB, MRN, SSN, etc.). All tools implement the following safeguards:

- Actual data values are never logged at INFO level or written to reports
- Only row counts, percentages, and field names are surfaced in output
- Reports containing HIPAA-flagged rules are marked with a privacy warning
- Column-level encryption recommendations are generated for PHI-bearing Delta writes

These are best-effort annotations — they do not substitute for a formal data classification review with your privacy officer.

---

## Repository layout

```
.
├── intake.py                    # Migration intake questionnaire
├── migration-intake.json        # Intake responses (per project)
├── run_pipeline.py              # Full pipeline runner (all steps in sequence)
├── TODOS.md                     # Remaining work backlog
├── docs/                        # Tool documentation
├── tests/                       # pytest suite + fixtures
│   └── fixtures/                # Sample XML exports, .prm files, validation YAML
└── tools/                       # Python packages
    ├── setup.py
    ├── requirements.txt
    ├── pc_extractor/            # XML parser, scorer, lineage, reporter
    ├── glue_gen/                # AWS Glue PySpark script + Terraform generator
    ├── databricks_gen/          # Databricks notebook + Terraform generator
    ├── param_translator/        # .prm parser, classifier, JSON/SSM exporter
    ├── validation_harness/      # Post-migration recon, schema diff, rules
    ├── workflow_gen/            # Workflow orchestration code generator
    └── review_gen/              # REVIEW mapping guidance document generator
```
