# TODOS — Remaining Work

Migration project: PC 10.5 → AWS Glue / Databricks
Deadline: Q3 2027
Team: 3–5 engineers, Python/PySpark/SQL skills, Azure DevOps CI/CD

Items are grouped by area and roughly ordered by dependency (earlier items unblock later ones). No estimates are given — scope each item in sprint planning with the team.

---

## 1. Packaging & developer experience

- [ ] **Add `setup.py` / `pyproject.toml` entry points for all tools**
  Currently only `pc-extractor` has a `console_scripts` entry point. `glue-gen`, `databricks-gen`, `param-translator`, and `validation-harness` must be run as `python -m ...`. Add proper entry points so they install as `glue-gen`, `databricks-gen`, `param-translator`, `validation-harness` commands.

- [ ] **Add a `requirements.txt` per tool** (or a root `requirements.txt` covering all)
  The current `tools/requirements.txt` is incomplete — it does not include drivers (pyodbc, databricks-sql-connector), PySpark, or delta-spark. Document which extras are needed per environment.

- [ ] **Top-level `README.md`**
  The repo has no README. Write a project README covering: what this repo is, the end-to-end workflow (intake → extract → generate → validate), prerequisites, and quick-start commands.

- [ ] **Pipeline runner / orchestration script**
  There is no single command that chains the full tool pipeline:
  `extract` → `param-translator parse` → `glue-gen generate-all` + `databricks-gen generate-all` → `validation-harness validate`
  Add a `run_pipeline.py` (or a `Makefile`) that runs the full sequence with a single invocation, respects a config file for paths, and checkpoints progress so it can resume after a partial failure.

---

## 2. Lineage CLI and output

- [ ] **Add `lineage` command to `pc-extractor`**
  Expose `trace_mapping` via a CLI subcommand, e.g.:
  ```
  pc-extractor lineage <manifest_file> <folder> <mapping> [--output-dir]
  ```
  Output formats: JSON (machine-readable), CSV (S2T spreadsheet), and a human-readable text table.

- [ ] **S2T CSV / Excel export function**
  The lineage module has no built-in output writer. Add `write_s2t_csv(lineage, path)` and optionally `write_s2t_excel(lineage, path)` (using openpyxl) to `pc_extractor/lineage.py` or a new `pc_extractor/s2t_exporter.py`. The docs/lineage.md has a manual example to port.

- [ ] **Bulk lineage run across all mappings**
  Add `pc-extractor lineage-all <manifest_file> --output-dir` that traces every mapping and writes one S2T file per mapping plus an aggregate cross-mapping lineage index (useful for impact analysis: "which mappings read from SRC_ORDERS?").

- [ ] **Mapplet resolution**
  `lineage.py` currently logs "reusable?" and returns `[]` for any instance whose transformation definition is not inline (reusable or mapplet). Resolve these by looking up the transformation definition in the folder's reusable section of the parsed XML. Requires `_parse_folder` to retain reusable transformations separately and `_Ctx` to accept them.

---

## 3. Code generation gaps

### glue-gen

- [ ] **Lookup transformation code generation**
  Connected lookups are detected and flagged but not generated. Implement PySpark lookup as a broadcast join: load the lookup table, join on the lookup condition, select the return fields. Handle both in-memory cache (small tables) and join (large tables).

- [ ] **Router: generate per-group filter branches**
  The current generator emits a TODO for Router transformations. Each GROUPATTR condition should become a `df.filter(...)` producing a separate DataFrame per group; each group's downstream target should be wired to its filtered DataFrame.

- [ ] **Normalizer code generation**
  Normalizer transposes row-level repeated groups into separate rows. Implement with `pyspark.sql.functions.explode` or a `flatMap`. The REF_SOURCE_FIELD lineage data is already parsed.

- [ ] **Aggregator GROUP BY code generation**
  Aggregator ports with `EXPRESSIONTYPE=GROUPBY` define the group-by keys; OUTPUT ports with aggregate expressions (`SUM(...)`, `COUNT(*)`) define the aggregate columns. Generate a `df.groupBy(...).agg(...)`.

- [ ] **Multi-source Joiner with complex conditions**
  Joiners with non-equi conditions or aliased column name conflicts need manual handling. Currently emits a TODO. Generate with explicit column aliasing and a complex join expression.

- [ ] **`--include-review` stub quality**
  REVIEW stubs currently include no transformation logic. Improve stubs to include source reads and a `# TODO:` skeleton for each transformation that needs manual work, matching the transformation type.

### databricks-gen

- [ ] **Excel and fixed-width flat file source reads**
  The PC source types `Excel` and `FLAT FILE` (fixed-width) appear in the intake. Implement `_source_read_code` branches for:
  - Excel: `spark.read.format("com.crealytics.spark.excel")...`
  - Fixed-width: read as raw text, apply `substring` columns per the field widths in `SourceDef.fields`

- [ ] **S3 flat file (CSV / delimited) source reads**
  Sources stored in S3 (landing zone) need `spark.read.csv(s3_path, ...)`. Add detection based on `db_type = "FLAT FILE"` + S3 path pattern in connection metadata.

- [ ] **Delta merge / upsert pattern**
  The current target write always uses `mode("append")`. Add a `load_type` parameter (append / overwrite / merge) and generate `DeltaTable.forPath(...).merge(...)` for upsert scenarios. This is common for dimension table loads.

- [ ] **Update Strategy handling**
  Update Strategy transformation marks rows as INSERT / UPDATE / DELETE / REJECT. Translate to:
  - INSERT / UPDATE → Delta merge
  - DELETE → `DeltaTable.delete(...)`
  - REJECT → write to a reject DataFrame / path

---

## 4. Parameter file integration

- [ ] **Inject `param-translator` output into Glue job configs**
  `glue-gen` generates `--JOB_ARG` placeholders but does not consume the `params-by-session.json` produced by `param-translator parse`. Wire the two: for each session task, look up its parameters and include them in the Terraform `default_arguments` block and the generated script's `getResolvedOptions` call.

- [ ] **Inject parameters into Databricks notebooks**
  Similarly, `databricks-gen` emits `dbutils.widgets.text(...)` stubs but does not know the actual parameter names and types. Consume `params-by-session.json` to generate correct widget definitions with classified types (DATE, SQL, PATH, INTEGER) and appropriate validation hints.

- [ ] **AWS SSM Parameter Store Terraform resources**
  Sensitive parameters (connection strings, passwords) should not be Terraform variables — they should be SSM SecureString parameters. Add a `param-translator export-ssm` command that generates `aws_ssm_parameter` Terraform resources for any parameter classified as a credential or on-prem path that has been migrated to a secret.

---

## 5. Multi-environment Terraform

- [ ] **Per-environment Terraform workspaces or modules**
  Generated Terraform currently targets a single environment. The intake specifies separate AWS accounts for dev / stage / prod. Refactor `tf_builder.py` to produce a module structure with environment-specific `tfvars` files:
  ```
  terraform/
    modules/
      glue_job/
    environments/
      dev/
        main.tf
        terraform.tfvars
      staging/
        ...
      prod/
        ...
  ```

- [ ] **Terraform state backend configuration**
  Add a `backend.tf` template (S3 + DynamoDB locking) for each environment. The bucket name and lock table name should come from `migration-intake.json` or a separate Terraform config file.

- [ ] **IAM role generation**
  Glue jobs and Databricks notebooks need IAM roles with least-privilege S3, Glue Catalog, and Secrets Manager permissions. Add a `terraform/iam/` module generated from the source/target DB types found in the manifest.

---

## 6. Workflow / scheduler migration

- [ ] **Workflow graph extraction**
  `WorkflowDef` is parsed (tasks, links, scheduler) but there is no code generator for workflows. The intake says the target orchestrator is "not decided yet." Add a `workflow-gen` tool (or subcommand of an existing tool) that can output:
  - AWS Step Functions state machine JSON
  - AWS Glue Workflow + Triggers
  - Apache Airflow DAG (for MWAA)
  - (stub for others when decided)

- [ ] **Schedule translation**
  PC `SCHEDULERINFO` uses a custom schedule format. Parse the schedule type and recurrence into a standard cron expression. The intake scheduler is "Other" (not ActiveBatch or UC4), so the exact source format is TBD — design the translator as a pluggable strategy.

- [ ] **Session-to-job mapping**
  `WorkflowDef.tasks` contains SESSION tasks with `mapping_ref`. When generating workflow code, wire each session task to the corresponding generated Glue job or Databricks notebook job.

---

## 7. Validation harness improvements

- [ ] **Auto-generate validation config from lineage**
  Given a `MappingLineage`, generate a skeleton `validation_config.yaml` with recon entries for each connected source/target table pair and null-check rules for primary key fields. This removes the manual config authoring step for straightforward mappings.

- [ ] **Value distribution checks**
  The intake requires "null rate and value distribution checks" but the current rule types don't include distribution comparison (e.g., verify that the histogram of ORDER_STATUS values is approximately the same in source and target). Add a `distribution_check` rule type.

- [ ] **Sign-off workflow**
  Add a `sign-off` command or output mode that produces a printable PDF / HTML report formatted for data owner review. Should include: table-level PASS/FAIL summary, sample row counts, failed rules with SQL, and a signature block.

- [ ] **Databricks connection support**
  The Databricks DSN format (`databricks://token:...`) needs the `databricks-sql-connector` SQLAlchemy dialect. Document this dependency and add a connection test command (`validation-harness test-connection <config>`).

---

## 8. REVIEW-platform mappings

- [ ] **REVIEW mapping guidance document generator**
  Mappings routed to REVIEW (score ≥ 9, stored procedures, custom transforms) currently get no output. Add a `review-gen` command that, for each REVIEW mapping, produces a structured Markdown migration guide containing: mapping summary, transformation inventory, stored procedure names, complexity reasons, and blank sections for the engineer to fill in.

- [ ] **Stored procedure migration path**
  Stored procedure calls in PC map to one of: (a) Databricks `CALL` statement, (b) rewrite as PySpark UDF, (c) leave on-prem and call via JDBC. Add a `--sp-strategy` flag to `databricks-gen` to choose the approach, and generate the appropriate code skeleton.

---

## 9. CI/CD (Azure DevOps)

- [ ] **Azure DevOps pipeline YAML for the toolchain**
  Add `azure-pipelines.yml` that on each PR:
  1. Runs `pytest tests/ -v`
  2. Runs `pc-extractor validate-xml exports/` (smoke test)
  3. Runs `pc-extractor extract` on sample fixtures
  4. Runs `glue-gen generate-all` and `databricks-gen generate-all` on the sample manifest
  5. Fails if any generation errors occur

- [ ] **Generated code linting in CI**
  After code generation, run `pylint --errors-only` or `ruff check` on all generated `.py` files to catch generation bugs (unclosed brackets, undefined variables, etc.).

- [ ] **Terraform `validate` and `plan` in CI**
  Run `terraform validate` on generated HCL. Add a Terraform plan step against a dev environment (requires Azure DevOps service connections to AWS).

- [ ] **Nightly validation run**
  Schedule a nightly Azure DevOps pipeline that runs `validation-harness validate` against dev source and target during the parallel run period. Archive dated reports as pipeline artifacts.

---

## 10. intake.py integration

- [ ] **Connect `migration-intake.json` to the toolchain**
  The `intake.py` questionnaire produces `migration-intake.json` but no tool currently reads it. Use it to:
  - Drive AWS region default in Terraform output (`aws_region` field)
  - Set default orchestrator target in `workflow-gen`
  - Gate HIPAA warnings based on `compliance_requirements` field
  - Pre-populate `validation-harness` connection DSN templates based on `source_databases` and `target_lakehouse` fields

---

## 11. Data type mapping

- [ ] **Audit and complete PC → PySpark / Delta type mapping matrix**
  The expression translator and code builders use ad-hoc type strings. Create a central `datatypes.py` module with a comprehensive PC datatype → Spark DataType mapping for all types seen in the intake's source databases (SQL Server `nvarchar`, `datetime2`, `money`, `uniqueidentifier`, etc.).

- [ ] **`uniqueidentifier` / UUID handling**
  SQL Server `uniqueidentifier` columns appear in healthcare source systems. Map to `StringType` in PySpark with a cast to lowercase and validation rule.

---

## 12. Known parser gaps

- [ ] **Reusable transformation resolution in XML parser**
  When `INSTANCE REUSABLE="YES"`, the transformation definition is in the repository's shared object library, not inline in the mapping XML. The parser currently stores `transformation_name` but does not look it up. Add a `reusable_transformations` dict to `FolderDef` populated from any `TRANSFORMATION REUSABLE="YES"` elements in the folder, and resolve them during `_parse_mapping`.

- [ ] **SHORTCUT elements**
  Some PC exports use `SHORTCUT` elements that are aliases to objects in other folders. The Java reference (`infa-s2t-gen`) handles these via `//SHORTCUT[@NAME=... and @OBJECTSUBTYPE='Target Definition']/@REFOBJECTNAME`. Add shortcut resolution to `xml_parser.py`.

- [ ] **MAPPLET transformation traversal in lineage**
  Mapplets expose INPUT and OUTPUT transformations at their boundary. The lineage traversal currently returns `[]` for mapplet instances. Implement traversal by entering the mapplet's internal connector graph via the INPUT transformation's ports.
