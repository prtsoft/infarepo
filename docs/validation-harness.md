# validation-harness

Runs post-migration data validation: row count reconciliation, schema diff, and business rule assertions. Driven by a YAML config file that describes the source/target connections and what to check.

This is the final gate before sign-off. The harness connects to both the source (PowerCenter-era) and target (Glue/Databricks) systems simultaneously and runs the configured checks.

## Prerequisites

- Both source and target databases must be reachable from where you run this tool.
- Connection strings use SQLAlchemy DSN format — the driver you specify must be installed.
- For Databricks targets, the Databricks SQL connector (`databricks-sql-connector`) must be installed.
- For SQL Server sources, `pyodbc` and the ODBC Driver 17 (or 18) for SQL Server must be installed.

Install the tool and extras:

```bash
pip install sqlalchemy pyodbc databricks-sql-connector pyyaml
```

## Configuration file

Create a YAML config file per migration target (one file per workflow or logical group):

```yaml
version: "1"

connections:
  source:
    dsn: "mssql+pyodbc://etl_user:${SOURCE_PASSWORD}@sqlserver-prod:1433/SalesDB?driver=ODBC+Driver+17+for+SQL+Server"
  target:
    dsn: "databricks://token:${DATABRICKS_TOKEN}@adb-1234.azuredatabricks.net/default"

validations:
  - source_table: "dbo.ORDERS"
    target_table: "sales_mart.fact_orders"

    recon:
      tolerance_pct: 0.5          # allow up to 0.5% row count difference
      source_sql: null            # optional: override COUNT(*) for source
      target_sql: null            # optional: override COUNT(*) for target

    schema_diff:
      enabled: true
      ignore_columns:             # columns present only in target (audit fields, etc.)
        - ETL_INSERT_DT
        - ETL_UPDATE_DT
        - LOAD_TS
      type_equivalences:          # treat these type pairs as compatible
        - [nvarchar, string]
        - [datetime2, timestamp]
        - [int, integer]
        - [bit, boolean]

    rules:
      - name: "no null order IDs"
        type: null_check
        column: ORDER_ID
        expect: not_null

      - name: "order amount non-negative"
        type: range_check
        column: ORDER_AMT
        min: 0

      - name: "unique order IDs"
        type: unique_check
        column: ORDER_ID

      - name: "valid status values"
        type: value_set
        column: STATUS
        allowed_values: [PENDING, APPROVED, CANCELLED, RETURNED]

      - name: "customer FK valid"
        type: referential
        column: CUSTOMER_ID
        parent_table: customers
        parent_column: CUSTOMER_ID

      - name: "no future order dates"
        type: custom_sql
        sql: "SELECT COUNT(*) FROM {table} WHERE ORDER_DATE > CURRENT_DATE"
        expect_zero: true
```

### Environment variables in connection strings

Connection strings support `${VAR_NAME}` substitution from environment variables. Never put passwords or tokens directly in the YAML file — use:

```bash
export SOURCE_PASSWORD="..."
export DATABRICKS_TOKEN="..."
validation-harness validate config/sales_mart_validation.yaml
```

---

## Commands

```bash
cd tools/
python -m validation_harness.cli <command> [OPTIONS]
```

### `validate` — full suite: recon + schema diff + rules

```
python -m validation_harness.cli validate <config> [OPTIONS]
```

Runs all configured checks and exits non-zero if any fail.

**Options**

| Option | Default | Description |
|---|---|---|
| `--output-dir`, `-o` | `output/validation` | Directory for report files. |
| `--dry-run` | false | Parse config and print plan without executing any SQL. |
| `--verbose`, `-v` | false | Debug logging. |

**Output files**

| File | Description |
|---|---|
| `validation-report.json` | Full results, rule details, counts. |
| `validation-summary.txt` | Human-readable summary for sign-off documentation. |

```bash
# Full validation
python -m validation_harness.cli validate \
  config/sales_mart_validation.yaml \
  --output-dir output/validation/

# Check the plan first without running SQL
python -m validation_harness.cli validate \
  config/sales_mart_validation.yaml \
  --dry-run
```

---

### `recon` — row count reconciliation only

```
python -m validation_harness.cli recon <config> [OPTIONS]
```

Faster than a full validate — only executes `COUNT(*)` on source and target. Use this for a quick sanity check during parallel run.

```bash
python -m validation_harness.cli recon config/sales_mart_validation.yaml
```

Sample output:

```
  [PASS]  dbo.ORDERS → sales_mart.fact_orders       src=1,248,631  tgt=1,248,601  delta=    -30 (-0.00%)
  [FAIL]  dbo.CUSTOMERS → sales_mart.dim_customer   src=   84,200  tgt=   71,005  delta=-13,195 (-15.67%)

  1/2 table(s) within tolerance.
```

---

### `diff-schema` — schema comparison only

```
python -m validation_harness.cli diff-schema <config> [OPTIONS]
```

Compares column names and data types between source and target without running row-level checks.

```bash
python -m validation_harness.cli diff-schema config/sales_mart_validation.yaml
```

---

### `report` — pretty-print an existing report

```
python -m validation_harness.cli report <validation-report.json>
```

Renders a prior report to the terminal. Use this to share results without re-running validation.

---

## Supported rule types

| `type` | Checks |
|---|---|
| `null_check` | Counts NULLs in `column`. Pass if 0 (with `expect: not_null`) or >0 (with `expect: null`). |
| `range_check` | Checks `column` values are within `min` / `max` bounds. |
| `unique_check` | Checks `column` has no duplicate values. |
| `value_set` | Checks all values in `column` are in `allowed_values`. |
| `referential` | Checks `column` in the target table has matching values in `parent_table.parent_column`. |
| `custom_sql` | Runs arbitrary SQL on the **target**; with `expect_zero: true`, fails if the result is non-zero. |

---

## HIPAA considerations

The harness detects PHI-bearing column names in rule configurations and flags them in the report summary. Specifically:

- Rule results for HIPAA-flagged columns do not log sample values.
- The output summary prints a warning: `N HIPAA-flagged rule(s) — review with your privacy officer before sharing the report.`
- `validation-summary.txt` is marked as containing potentially sensitive metadata.

Do not send `validation-report.json` to external parties without redacting PHI-adjacent context.

---

## Python API

```python
from validation_harness.runner import run_validation
from validation_harness.reporter import write_json_report, write_text_summary
from pathlib import Path
import os

os.environ["SOURCE_PASSWORD"] = "..."
os.environ["DATABRICKS_TOKEN"] = "..."

report = run_validation(Path("config/sales_mart_validation.yaml"))
write_json_report(report, Path("output/validation/"))
write_text_summary(report, Path("output/validation/"))

s = report.summary
print(f"Overall: {'PASS' if s.overall_passed else 'FAIL'}")
print(f"Recon: {s.recon_passed}/{s.recon_total}")
print(f"Schema: {s.schema_diff_passed}/{s.schema_diff_total}")
print(f"Rules: {s.rules_passed}/{s.rules_total}")
```

---

## Running during parallel operation

During the 2–3 month parallel run period, run validation on a schedule:

```bash
# Daily recon check (fast)
python -m validation_harness.cli recon config/sales_mart_validation.yaml

# Full validation before sign-off
python -m validation_harness.cli validate config/sales_mart_validation.yaml \
  --output-dir "output/validation/$(date +%Y-%m-%d)/"
```

Keep report directories dated so you can track validation drift over time.
