"""
Databricks Python notebook builder.

Generates .py-format Databricks notebooks (cells separated by
# COMMAND ----------) from a MappingDef.

HIPAA note: generated notebooks never log actual data values — only
row counts, table names, and timestamps.
"""
from __future__ import annotations

import sys
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pc_extractor.models import (
    MappingDef,
    SourceDef,
    TargetDef,
    TransformationDef,
    TransformationType,
)
from .models import DatabricksNotebook, NotebookCell

# Try to import expr_translator from glue_gen
try:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from glue_gen.expr_translator import translate, translate_filter
    _HAS_EXPR_TRANSLATOR = True
except ImportError:
    _HAS_EXPR_TRANSLATOR = False


# ---------------------------------------------------------------------------
# PHI column name heuristics (HIPAA)
# ---------------------------------------------------------------------------

_PHI_PATTERNS = re.compile(
    r"(patient|member|ssn|dob|birth|name|address|phone|email|mrn|npi|"
    r"diagnosis|medication|insurance|claim|provider|discharge|admit)",
    re.IGNORECASE,
)


def _has_phi_columns(mapping: MappingDef) -> bool:
    """Return True if any target field name looks like PHI."""
    for t in mapping.transformations:
        for port in t.ports:
            if _PHI_PATTERNS.search(port.name):
                return True
    # Also check source fields embedded in instances list (names from mapping)
    return False


def _phi_in_field_names(field_names: List[str]) -> bool:
    return any(_PHI_PATTERNS.search(f) for f in field_names)


def _safe_var(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", name.lower()).strip("_")


# ---------------------------------------------------------------------------
# Expression translation helper
# ---------------------------------------------------------------------------

def _translate_expr(expr: str) -> Tuple[str, bool]:
    """
    Translate a PC expression to PySpark.
    Returns (translated_expr, was_translated).
    If expr_translator unavailable, returns raw expr as comment.
    """
    if not expr or not expr.strip():
        return "None", True
    if _HAS_EXPR_TRANSLATOR:
        result = translate(expr)
        return result.pyspark_expr, True
    else:
        return f"# PC expr (translate manually): {expr}", False


def _translate_filter_expr(expr: str) -> Tuple[str, bool]:
    if not expr or not expr.strip():
        return "True", True
    if _HAS_EXPR_TRANSLATOR:
        result = translate_filter(expr)
        return result.pyspark_expr, True
    else:
        return f"# PC filter (translate manually): {expr}", False


# ---------------------------------------------------------------------------
# Source read code generation
# ---------------------------------------------------------------------------

def _source_read_code(src_name: str, db_type: str, table_name: str,
                      sql_override: Optional[str]) -> List[str]:
    """Generate PySpark code lines to read a source DataFrame."""
    db_upper = db_type.upper() if db_type else ""
    lines: List[str] = []
    var = _safe_var(src_name)

    if db_upper in ("SQLSERVER", "MICROSOFT SQL SERVER", "SQL SERVER"):
        table_or_query = sql_override if sql_override else table_name
        # Use sub-query wrapping for sql_override
        if sql_override:
            dbtable_expr = f"({sql_override}) AS sq_{var}"
        else:
            dbtable_expr = table_name
        lines += [
            f"# Source: {src_name} ({db_type})",
            f'jdbc_url_{var} = (',
            f'    f"jdbc:sqlserver://{{dbutils.secrets.get(\'migration\', \'sql_host\')}}"',
            f'    f";databaseName={{dbutils.secrets.get(\'migration\', \'sql_db\')}}"',
            f')',
            f'df_{var} = (',
            f'    spark.read.format("jdbc")',
            f'    .option("url", jdbc_url_{var})',
            f'    .option("dbtable", "{dbtable_expr}")',
            f'    .option("user", dbutils.secrets.get("migration", "sql_user"))',
            f'    .option("password", dbutils.secrets.get("migration", "sql_password"))',
            f'    .load()',
            f')',
        ]

    elif db_upper in ("ORACLE",):
        table_or_query = sql_override if sql_override else table_name
        if sql_override:
            dbtable_expr = f"({sql_override}) sq_{var}"
        else:
            dbtable_expr = table_name
        lines += [
            f"# Source: {src_name} ({db_type})",
            f'jdbc_url_{var} = (',
            f'    f"jdbc:oracle:thin:@{{dbutils.secrets.get(\'migration\', \'oracle_host\')}}"',
            f'    f":{{dbutils.secrets.get(\'migration\', \'oracle_port\')}}"',
            f'    f"/{{dbutils.secrets.get(\'migration\', \'oracle_db\')}}"',
            f')',
            f'df_{var} = (',
            f'    spark.read.format("jdbc")',
            f'    .option("url", jdbc_url_{var})',
            f'    .option("dbtable", "{dbtable_expr}")',
            f'    .option("user", dbutils.secrets.get("migration", "oracle_user"))',
            f'    .option("password", dbutils.secrets.get("migration", "oracle_password"))',
            f'    .load()',
            f')',
        ]

    elif db_upper in ("S3_CSV", "FLAT FILE", "FILE"):
        s3_path_param = f"s3_input_path_{var}"
        lines += [
            f"# Source: {src_name} ({db_type}) — S3 CSV",
            f'{s3_path_param} = params.get("s3_input_path", "s3://BUCKET/PATH/")  # TODO: set correct S3 path',
            f'df_{var} = (',
            f'    spark.read.format("csv")',
            f'    .option("header", "true")',
            f'    .option("inferSchema", "true")',
            f'    .load({s3_path_param})',
            f')',
        ]

    elif db_upper in ("FIXED_WIDTH",):
        s3_path_param = f"s3_input_path_{var}"
        lines += [
            f"# Source: {src_name} ({db_type}) — Fixed-Width file",
            f'# TODO: define field positions for fixed-width parsing',
            f'{s3_path_param} = params.get("s3_input_path", "s3://BUCKET/PATH/")  # TODO: set correct S3 path',
            f'df_{var} = (',
            f'    spark.read.format("text")',
            f'    .load({s3_path_param})',
            f')',
            f'# TODO: extract fixed-width columns using F.substring()',
        ]

    elif db_upper in ("EXCEL",):
        s3_path_param = f"s3_input_path_{var}"
        lines += [
            f"# Source: {src_name} ({db_type}) — Excel",
            f'# TODO: install com.crealytics:spark-excel library on cluster',
            f'{s3_path_param} = params.get("s3_input_path", "s3://BUCKET/PATH/")  # TODO: set correct S3 path',
            f'df_{var} = (',
            f'    spark.read.format("com.crealytics.spark.excel")',
            f'    .option("useHeader", "true")',
            f'    .load({s3_path_param})',
            f')',
        ]

    elif db_upper in ("DELTA", "DATABRICKS", "S3_DELTA"):
        delta_table = table_name or src_name
        lines += [
            f"# Source: {src_name} ({db_type}) — Delta table",
            f'df_{var} = spark.table("{delta_table}")',
        ]

    elif db_upper in ("S3_PARQUET",):
        s3_path_param = f"s3_input_path_{var}"
        lines += [
            f"# Source: {src_name} ({db_type}) — S3 Parquet",
            f'{s3_path_param} = params.get("s3_input_path", "s3://BUCKET/PATH/")  # TODO: set correct S3 path',
            f'df_{var} = spark.read.format("parquet").load({s3_path_param})',
        ]

    else:
        lines += [
            f"# Source: {src_name} ({db_type}) — UNKNOWN source type, manual implementation required",
            f'# TODO: implement read for {db_type}',
            f'df_{var} = None  # TODO',
        ]

    return lines


# ---------------------------------------------------------------------------
# Transformation code generation
# ---------------------------------------------------------------------------

def _transformation_code(
    t: TransformationDef,
    input_var: str,
    warnings: List[str],
) -> Tuple[List[str], str]:
    """
    Generate code lines for a single transformation.
    Returns (lines, output_var_name).
    """
    out_var = _safe_var(t.name)
    ttype = t.type
    lines: List[str] = [f"# Transformation: {t.name} ({ttype.value})"]

    if ttype == TransformationType.SOURCE_QUALIFIER:
        # SOURCE_QUALIFIER with sql_query override handled in source read cell;
        # here we just alias the input DataFrame
        lines.append(f"df_{out_var} = df_{input_var}  # SOURCE_QUALIFIER passthrough")

    elif ttype == TransformationType.EXPRESSION:
        lines.append(f"df_{out_var} = df_{input_var}")
        # expressions from ports
        for port in t.ports:
            if port.expression and port.expression.strip():
                translated, ok = _translate_expr(port.expression)
                if ok:
                    lines.append(f'df_{out_var} = df_{out_var}.withColumn("{port.name}", {translated})')
                else:
                    lines.append(f'# {translated}')
                    lines.append(f'# df_{out_var} = df_{out_var}.withColumn("{port.name}", ...)  # TODO')
                    warnings.append(f"Expression '{port.expression}' for port '{port.name}' needs manual translation.")

    elif ttype == TransformationType.FILTER:
        cond = t.filter_condition or ""
        translated, ok = _translate_filter_expr(cond)
        if ok:
            lines.append(f'df_{out_var} = df_{input_var}.filter(F.expr("{translated}"))')
        else:
            lines.append(f'# {translated}')
            lines.append(f'df_{out_var} = df_{input_var}  # TODO: apply filter manually')
            warnings.append(f"Filter condition '{cond}' needs manual translation.")

    elif ttype == TransformationType.AGGREGATOR:
        # Group-by ports (with port_type INPUT) vs agg ports
        group_cols = [p.name for p in t.ports if p.port_type in ("", "INPUT", "INPUT/OUTPUT") and not p.expression]
        agg_exprs = []
        for port in t.ports:
            if port.expression and port.expression.strip():
                translated, _ = _translate_expr(port.expression)
                agg_exprs.append(f'{translated}.alias("{port.name}")')
        if group_cols:
            group_str = ", ".join(f'"{c}"' for c in group_cols)
            lines.append(f'df_{out_var} = df_{input_var}.groupBy({group_str})')
        else:
            lines.append(f'df_{out_var} = df_{input_var}.groupBy()')
        if agg_exprs:
            agg_str = ",\n        ".join(agg_exprs)
            lines.append(f'df_{out_var} = df_{out_var}.agg(')
            lines.append(f'    {agg_str}')
            lines.append(f')')
        else:
            lines.append(f'# TODO: define aggregation expressions')
            warnings.append(f"Aggregator '{t.name}' has no expression ports — define agg() manually.")

    elif ttype == TransformationType.JOINER:
        cond = t.join_condition or ""
        jtype = (t.join_type or "").lower()
        spark_how = "inner"
        if "outer" in jtype:
            if "master" in jtype or "left" in jtype:
                spark_how = "left"
            elif "detail" in jtype or "right" in jtype:
                spark_how = "right"
            elif "full" in jtype:
                spark_how = "outer"
        if cond:
            translated, ok = _translate_expr(cond)
            lines += [
                f'# TODO: replace df_left / df_right with actual DataFrames',
                f'df_{out_var} = df_left.join(df_right, {translated}, how="{spark_how}")',
            ]
        else:
            lines += [
                f'# TODO: define join condition',
                f'df_{out_var} = df_left.join(df_right, "UNKNOWN_JOIN_KEY", how="{spark_how}")  # TODO',
            ]
        warnings.append(f"Joiner '{t.name}': verify df_left/df_right variable names and join key.")

    elif ttype == TransformationType.LOOKUP:
        cond = t.lookup_condition or ""
        lines += [
            f'# LOOKUP — broadcast join pattern',
            f'# TODO: define lookup_df (the lookup table)',
            f'df_{out_var} = df_{input_var}.join(F.broadcast(lookup_df_{out_var}), {repr(cond) if cond else "[]"}, how="left")',
        ]
        warnings.append(f"Lookup '{t.name}': define lookup_df_{out_var} and join key.")

    else:
        lines += [
            f'# TODO: transformation type "{ttype.value}" not auto-generated',
            f'df_{out_var} = df_{input_var}  # TODO: implement {t.name}',
        ]
        warnings.append(f"Transformation '{t.name}' of type '{ttype.value}' requires manual implementation.")

    return lines, out_var


# ---------------------------------------------------------------------------
# Write cell generation
# ---------------------------------------------------------------------------

def _write_cell_code(target_name: str, db_type: str, table_name: str,
                     load_type: str, df_var: str) -> List[str]:
    """Generate Delta write code based on load_type."""
    lines: List[str] = [f"# Target: {target_name} ({db_type}) — load_type: {load_type}"]
    lt = (load_type or "insert").lower()
    tbl = table_name or target_name

    if lt == "upsert":
        # Need merge keys — use the first key field or fallback
        lines += [
            f'target_table = f"{tbl}"',
            f'# TODO: replace "id_column" with actual merge key column(s)',
            f'merge_condition = "t.id_column = s.id_column"  # TODO: set correct merge key',
            f'',
            f'DeltaTable.forName(spark, target_table).alias("t").merge(',
            f'    df_final.alias("s"),',
            f'    merge_condition,',
            f').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()',
        ]
    elif lt in ("overwrite", "truncate_insert"):
        lines += [
            f'target_table = f"{tbl}"',
            f'(',
            f'    df_final.write',
            f'    .format("delta")',
            f'    .mode("overwrite")',
            f'    .saveAsTable(target_table)',
            f')',
        ]
    else:
        # insert / append
        lines += [
            f'target_table = f"{tbl}"',
            f'(',
            f'    df_final.write',
            f'    .format("delta")',
            f'    .mode("append")',
            f'    .saveAsTable(target_table)',
            f')',
        ]
    return lines


# ---------------------------------------------------------------------------
# Main builder class
# ---------------------------------------------------------------------------

class DatabricksNotebookBuilder:
    """
    Builds a Databricks Python notebook (.py format) from a MappingDef.
    """

    def build(self, mapping: MappingDef) -> DatabricksNotebook:
        warnings: List[str] = []
        cells: List[NotebookCell] = []

        # Collect field names for PHI check
        all_field_names: List[str] = []
        for t in mapping.transformations:
            all_field_names += [p.name for p in t.ports]

        has_phi = _phi_in_field_names(all_field_names)

        # 1. Header cell (markdown)
        cells.append(self._header_cell(mapping, has_phi))

        # 2. Imports cell
        cells.append(self._imports_cell())

        # 3. Parameters cell
        cells.append(self._params_cell(mapping))

        # 4. Source read cell
        src_cell_lines, src_vars = self._source_read_cell(mapping, warnings)
        cells.append(NotebookCell("code", "\n".join(src_cell_lines)))

        # 5. Transform cell
        transform_lines, final_var, t_warnings = self._transform_cell(mapping, src_vars)
        warnings += t_warnings
        cells.append(NotebookCell("code", "\n".join(transform_lines)))

        # 6. Write cell
        write_lines = self._write_cell(mapping, final_var, warnings)
        cells.append(NotebookCell("code", "\n".join(write_lines)))

        # 7. Summary cell
        cells.append(self._summary_cell(mapping))

        if has_phi:
            warnings.append("PHI-like column names detected — verify HIPAA compliance before deploying.")

        return DatabricksNotebook(
            mapping_name=mapping.name,
            folder=mapping.folder,
            cells=cells,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Cell builders
    # ------------------------------------------------------------------

    def _header_cell(self, mapping: MappingDef, has_phi: bool) -> NotebookCell:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        source_systems = ", ".join(
            t.name for t in mapping.transformations
            if t.type == TransformationType.SOURCE_QUALIFIER
        ) or "N/A"
        lines = [
            f"# {mapping.name}",
            f"",
            f"**Folder:** {mapping.folder}  ",
            f"**Generated:** {now}  ",
            f"**Migrated from:** Informatica PowerCenter 10.5  ",
            f"**Source system(s):** {source_systems}  ",
        ]
        if has_phi:
            lines += [
                f"",
                f"**HIPAA NOTICE:** This notebook processes PHI-sensitive columns.  ",
                f"Do not log, print, or display actual data values in production.  ",
                f"Ensure cluster is in a HIPAA-compliant workspace with audit logging enabled.  ",
            ]
        return NotebookCell("markdown", "\n".join(lines))

    def _imports_cell(self) -> NotebookCell:
        code = (
            "from pyspark.sql import functions as F\n"
            "from pyspark.sql.types import *\n"
            "from delta.tables import DeltaTable\n"
        )
        return NotebookCell("code", code.rstrip())

    def _params_cell(self, mapping: MappingDef) -> NotebookCell:
        lines = [
            'dbutils.widgets.text("env", "dev")',
            'dbutils.widgets.text("catalog", "main")',
            'dbutils.widgets.text("schema", "default")',
        ]
        param_keys = ["env", "catalog", "schema"]

        # Add any mapping variables that are parameters
        for var in mapping.variables:
            if var.is_param:
                safe_key = var.name.lstrip("$").lower()
                default = var.default_value or ""
                lines.append(f'dbutils.widgets.text("{safe_key}", "{default}")')
                param_keys.append(safe_key)

        keys_str = ", ".join(f'"{k}"' for k in param_keys)
        lines.append(f'params = {{k: dbutils.widgets.get(k) for k in [{keys_str}]}}')
        return NotebookCell("code", "\n".join(lines))

    def _source_read_cell(
        self, mapping: MappingDef, warnings: List[str]
    ) -> Tuple[List[str], List[str]]:
        """Returns (lines, list_of_df_var_names)."""
        lines: List[str] = []
        src_vars: List[str] = []

        # Find SOURCE_QUALIFIER transformations
        sq_transforms = [
            t for t in mapping.transformations
            if t.type == TransformationType.SOURCE_QUALIFIER
        ]

        if sq_transforms:
            for t in sq_transforms:
                var = _safe_var(t.name)
                src_vars.append(var)
                sql_q = t.sql_query or ""
                # Try to infer db_type from mapping instances (use first source in mapping)
                db_type = "SQLSERVER"  # default
                table_name = t.name

                # Check if instances list has source info
                for inst in mapping.instances:
                    if inst.name == t.name or inst.transformation_name == t.name:
                        db_type = inst.transformation_type or db_type
                        break

                src_lines = _source_read_code(t.name, db_type, table_name, sql_q)
                lines += src_lines
                lines.append("")
        else:
            # No SOURCE_QUALIFIER — fall back: one entry per instance name (source)
            for src_name in (mapping.sources or []):
                var = _safe_var(src_name)
                src_vars.append(var)
                lines += [
                    f"# Source: {src_name} — db_type unknown, manual implementation required",
                    f"# TODO: implement read for source '{src_name}'",
                    f"df_{var} = None  # TODO",
                    "",
                ]
                warnings.append(f"Source '{src_name}' has no SOURCE_QUALIFIER — implement read manually.")

        if not src_vars:
            src_vars = ["src"]
            lines += ["# No sources detected", "df_src = None  # TODO"]

        return lines, src_vars

    def _transform_cell(
        self, mapping: MappingDef, src_vars: List[str]
    ) -> Tuple[List[str], str, List[str]]:
        """Returns (lines, final_df_var, warnings)."""
        warnings: List[str] = []
        lines: List[str] = []
        current_var = src_vars[0] if src_vars else "src"

        non_sq = [
            t for t in mapping.transformations
            if t.type != TransformationType.SOURCE_QUALIFIER
        ]

        if not non_sq:
            lines.append(f"# No transformations to apply")
            lines.append(f"df_final = df_{current_var}")
            return lines, "final", warnings

        for t in non_sq:
            t_lines, out_var = _transformation_code(t, current_var, warnings)
            lines += t_lines
            lines.append("")
            current_var = out_var

        lines.append(f"df_final = df_{current_var}")
        return lines, "final", warnings

    def _write_cell(
        self, mapping: MappingDef, df_var: str, warnings: List[str]
    ) -> List[str]:
        lines: List[str] = []
        if not mapping.targets:
            lines += [
                "# No target definitions found",
                "# TODO: implement write",
            ]
            warnings.append("No target definitions found in mapping.")
            return lines

        # For now handle first target (multiple targets → TODO)
        if len(mapping.targets) > 1:
            warnings.append("Multiple targets detected — only first target is generated. Implement remaining targets manually.")

        tgt_name = mapping.targets[0]

        # Look for TargetDef in instances (mapping.targets contains instance names in real model)
        # In the real MappingDef the targets list contains instance names (str)
        # We write to a delta table named after the target
        # Find TransformationDef for this target if it's an OUTPUT type
        tgt_table = tgt_name
        load_type = "insert"
        db_type = "DELTA"

        # Look for OUTPUT transformation matching this target name
        for t in mapping.transformations:
            if t.type == TransformationType.OUTPUT and t.name == tgt_name:
                break

        write_lines = _write_cell_code(tgt_name, db_type, tgt_table, load_type, df_var)
        lines += write_lines
        return lines

    def _summary_cell(self, mapping: MappingDef) -> NotebookCell:
        target = mapping.targets[0] if mapping.targets else "unknown_target"
        code = (
            "# HIPAA: no PII logged\n"
            f'print(f"Loaded {{df_final.count()}} rows into {target}")\n'
            f'print(f"Completed at: {{__import__(\'datetime\').datetime.now().__import__(\'datetime\').timezone.utc}}")'
        )
        # Simpler version:
        code = (
            "# HIPAA: no PII logged\n"
            f'print(f"Loaded {{df_final.count()}} rows into \\"{target}\\"")'
        )
        return NotebookCell("code", code)


# ---------------------------------------------------------------------------
# Notebook serializer
# ---------------------------------------------------------------------------

_CELL_SEP = "# COMMAND ----------"


def render_notebook(notebook: DatabricksNotebook) -> str:
    """Render a DatabricksNotebook to .py format string."""
    parts: List[str] = []
    for i, cell in enumerate(notebook.cells):
        if cell.cell_type == "markdown":
            # Databricks markdown cell: # MAGIC %md\n# MAGIC <content>
            md_lines = ["# MAGIC %md"]
            for line in cell.source.splitlines():
                md_lines.append(f"# MAGIC {line}" if line else "# MAGIC ")
            parts.append("\n".join(md_lines))
        else:
            parts.append(cell.source)
    return f"\n{_CELL_SEP}\n".join(parts) + "\n"
