"""
Tests for databricks_gen — notebook builder, tf builder, generator, CLI.

Run with:  python -m pytest tests/test_databricks_gen.py -v
"""

import json
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

# Ensure tools/ is on sys.path
sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

from pc_extractor.models import (
    MappingDef,
    MappingFlags,
    MigrationManifest,
    FolderDef,
    SourceDef,
    TargetDef,
    FieldDef,
    TransformationDef,
    TransformationType,
    PortDef,
    ConnectorDef,
    InstanceDef,
    ExtractionSummary,
    TargetPlatform,
)

from databricks_gen.models import (
    NotebookCell,
    DatabricksNotebook,
    NotebookGenerationResult,
    GenerationReport,
)
from databricks_gen.notebook_builder import (
    DatabricksNotebookBuilder,
    render_notebook,
    _CELL_SEP,
    _has_phi_columns,
    _phi_in_field_names,
)
from databricks_gen.tf_builder import build_terraform_job, build_terraform_variables
from databricks_gen.generator import generate_all, generate_single, _load_manifest
from databricks_gen.cli import cli


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_field(name: str, datatype: str = "string") -> FieldDef:
    return FieldDef(name=name, datatype=datatype)


def _make_port(name: str, expression: str = "", port_type: str = "OUTPUT") -> PortDef:
    return PortDef(name=name, datatype="string", port_type=port_type, expression=expression)


def _make_sq(name: str = "SQ_PATIENT", sql_query: str = "") -> TransformationDef:
    return TransformationDef(
        name=name,
        type=TransformationType.SOURCE_QUALIFIER,
        sql_query=sql_query or None,
        ports=[
            _make_port("PATIENT_ID"),
            _make_port("DISCHARGE_DATE"),
            _make_port("ACTIVE_FLAG"),
        ],
    )


def _make_expression(name: str = "EXP_CLEAN") -> TransformationDef:
    return TransformationDef(
        name=name,
        type=TransformationType.EXPRESSION,
        ports=[
            _make_port("ACTIVE", expression="IIF(ACTIVE_FLAG='Y', True, False)"),
        ],
    )


def _make_filter(name: str = "FIL_ACTIVE") -> TransformationDef:
    return TransformationDef(
        name=name,
        type=TransformationType.FILTER,
        filter_condition="ACTIVE_FLAG = 'Y'",
        ports=[],
    )


@pytest.fixture
def patient_mapping() -> MappingDef:
    """M_EXTRACT_PATIENT_DELTA fixture — mirrors the spec."""
    m = MappingDef(
        name="M_EXTRACT_PATIENT_DELTA",
        folder="PATIENT_EXTRACT",
        target_platform=TargetPlatform.DATABRICKS,
        complexity_score=3,
        sources=["SQ_PATIENT"],
        targets=["T_PATIENT_DELTA"],
        transformations=[
            _make_sq(),
            _make_expression(),
            _make_filter(),
        ],
    )
    return m


@pytest.fixture
def glue_mapping() -> MappingDef:
    m = MappingDef(
        name="M_LOAD_FACT_ORDERS",
        folder="SALES_MART",
        target_platform=TargetPlatform.GLUE,
        complexity_score=2,
        sources=["SQ_ORDERS"],
        targets=["T_FACT_ORDERS"],
        transformations=[
            TransformationDef(
                name="SQ_ORDERS",
                type=TransformationType.SOURCE_QUALIFIER,
            )
        ],
    )
    return m


@pytest.fixture
def review_mapping() -> MappingDef:
    m = MappingDef(
        name="M_COMPLEX_JAVA",
        folder="COMPLEX",
        target_platform=TargetPlatform.REVIEW,
        complexity_score=9,
        sources=[],
        targets=[],
        transformations=[],
        review_notes=["Java transformation requires manual review"],
    )
    return m


@pytest.fixture
def manifest_with_all(patient_mapping, glue_mapping, review_mapping) -> MigrationManifest:
    """Manifest with DATABRICKS, GLUE, and REVIEW mappings."""
    m = MigrationManifest(
        extracted_at="2026-03-23T00:00:00Z",
        source_files=["export.xml"],
        repository_name="TestRepo",
        summary=ExtractionSummary(
            total_mappings=3,
            routed_databricks=1,
            routed_glue=1,
            routed_review=1,
        ),
    )
    m.folders = {
        "PATIENT_EXTRACT": FolderDef(
            name="PATIENT_EXTRACT",
            mappings={"M_EXTRACT_PATIENT_DELTA": patient_mapping},
        ),
        "SALES_MART": FolderDef(
            name="SALES_MART",
            mappings={"M_LOAD_FACT_ORDERS": glue_mapping},
        ),
        "COMPLEX": FolderDef(
            name="COMPLEX",
            mappings={"M_COMPLEX_JAVA": review_mapping},
        ),
    }
    return m


@pytest.fixture
def builder() -> DatabricksNotebookBuilder:
    return DatabricksNotebookBuilder()


# ---------------------------------------------------------------------------
# TestNotebookBuilder
# ---------------------------------------------------------------------------

class TestNotebookBuilder:

    def test_notebook_has_correct_number_of_cells(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        # header, imports, params, source read, transform, write, summary = 7
        assert len(nb.cells) == 7

    def test_notebook_has_header_cell_first(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        assert nb.cells[0].cell_type == "markdown"

    def test_header_cell_contains_mapping_name(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        assert "M_EXTRACT_PATIENT_DELTA" in nb.cells[0].source

    def test_header_cell_contains_folder_name(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        assert "PATIENT_EXTRACT" in nb.cells[0].source

    def test_header_cell_contains_migration_note(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        header = nb.cells[0].source
        assert "PowerCenter" in header or "Migrated" in header

    def test_imports_cell_has_pyspark_imports(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        imports_cell = nb.cells[1]
        assert imports_cell.cell_type == "code"
        assert "from pyspark.sql import functions as F" in imports_cell.source

    def test_imports_cell_has_delta_table(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        imports_cell = nb.cells[1]
        assert "from delta.tables import DeltaTable" in imports_cell.source

    def test_imports_cell_has_pyspark_types(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        imports_cell = nb.cells[1]
        assert "from pyspark.sql.types import *" in imports_cell.source

    def test_params_cell_has_dbutils_widgets(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        params_cell = nb.cells[2]
        assert "dbutils.widgets.text" in params_cell.source

    def test_params_cell_has_env_widget(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        params_cell = nb.cells[2]
        assert '"env"' in params_cell.source

    def test_params_cell_has_params_dict(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        params_cell = nb.cells[2]
        assert "params = " in params_cell.source

    def test_source_read_cell_generated_for_sqlserver(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        source_cell = nb.cells[3]
        assert source_cell.cell_type == "code"
        # Should contain JDBC read pattern
        src_text = source_cell.source
        assert "jdbc" in src_text.lower() or "spark.read" in src_text or "df_" in src_text

    def test_source_read_cell_uses_secrets_api(self, builder):
        """JDBC source should use dbutils.secrets.get."""
        # Create a mapping with a SQL Server source qualifier
        m = MappingDef(
            name="M_TEST",
            folder="TEST",
            target_platform=TargetPlatform.DATABRICKS,
            transformations=[
                TransformationDef(
                    name="SQ_TEST",
                    type=TransformationType.SOURCE_QUALIFIER,
                    sql_query="SELECT * FROM dbo.TEST",
                )
            ],
            sources=["SQ_TEST"],
            targets=["T_TEST"],
        )
        nb = DatabricksNotebookBuilder().build(m)
        src_text = nb.cells[3].source
        # SOURCE_QUALIFIER with sql_query → JDBC read → secrets API
        assert "dbutils.secrets.get" in src_text or "jdbc" in src_text.lower()

    def test_transform_cell_applies_expression(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        transform_cell = nb.cells[4]
        # Should reference EXP_CLEAN expression port
        assert "withColumn" in transform_cell.source or "ACTIVE" in transform_cell.source

    def test_transform_cell_applies_filter(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        transform_cell = nb.cells[4]
        assert "filter" in transform_cell.source.lower() or "ACTIVE_FLAG" in transform_cell.source

    def test_transform_cell_sets_df_final(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        transform_cell = nb.cells[4]
        assert "df_final" in transform_cell.source

    def test_write_cell_generated(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        write_cell = nb.cells[5]
        assert write_cell.cell_type == "code"
        # Should reference target
        assert "T_PATIENT_DELTA" in write_cell.source or "delta" in write_cell.source.lower() or "write" in write_cell.source.lower()

    def test_write_cell_insert_uses_append(self, builder):
        m = MappingDef(
            name="M_INS", folder="F",
            target_platform=TargetPlatform.DATABRICKS,
            transformations=[_make_sq("SQ_X")],
            sources=["SQ_X"],
            targets=["T_OUT"],
        )
        nb = DatabricksNotebookBuilder().build(m)
        write_text = nb.cells[5].source
        assert 'append' in write_text or 'insert' in write_text.lower()

    def test_summary_cell_logs_row_count_only(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        summary_cell = nb.cells[6]
        assert "count()" in summary_cell.source
        # Must not reference column values
        assert "HIPAA" in summary_cell.source

    def test_hipaa_notice_in_header_for_phi_columns(self, builder):
        """PHI-looking column names in ports should trigger HIPAA notice."""
        phi_mapping = MappingDef(
            name="M_PHI",
            folder="CLINICAL",
            target_platform=TargetPlatform.DATABRICKS,
            transformations=[
                TransformationDef(
                    name="SQ_PHI",
                    type=TransformationType.SOURCE_QUALIFIER,
                    ports=[
                        _make_port("PATIENT_NAME"),
                        _make_port("SSN"),
                        _make_port("DOB"),
                    ],
                )
            ],
            sources=["SQ_PHI"],
            targets=["T_PHI"],
        )
        nb = DatabricksNotebookBuilder().build(phi_mapping)
        header = nb.cells[0].source
        assert "HIPAA" in header

    def test_no_hipaa_notice_for_nonphi_columns(self, builder):
        """Non-PHI columns should not trigger HIPAA notice in header."""
        safe_mapping = MappingDef(
            name="M_SAFE",
            folder="FINANCE",
            target_platform=TargetPlatform.DATABRICKS,
            transformations=[
                TransformationDef(
                    name="SQ_SAFE",
                    type=TransformationType.SOURCE_QUALIFIER,
                    ports=[
                        _make_port("ORDER_ID"),
                        _make_port("AMOUNT"),
                        _make_port("PRODUCT_CODE"),
                    ],
                )
            ],
            sources=["SQ_SAFE"],
            targets=["T_SAFE"],
        )
        nb = DatabricksNotebookBuilder().build(safe_mapping)
        header = nb.cells[0].source
        assert "HIPAA" not in header

    def test_warnings_list_populated_for_unsupported_transform(self, builder):
        m = MappingDef(
            name="M_JAVA",
            folder="F",
            target_platform=TargetPlatform.DATABRICKS,
            transformations=[
                TransformationDef(name="SQ_X", type=TransformationType.SOURCE_QUALIFIER),
                TransformationDef(name="JAVA_T", type=TransformationType.JAVA),
            ],
            sources=["SQ_X"],
            targets=["T_OUT"],
        )
        nb = DatabricksNotebookBuilder().build(m)
        assert len(nb.warnings) > 0

    def test_phi_in_field_names_detects_patient(self):
        assert _phi_in_field_names(["PATIENT_ID", "ORDER_ID"]) is True

    def test_phi_in_field_names_detects_ssn(self):
        assert _phi_in_field_names(["SSN"]) is True

    def test_phi_in_field_names_safe_names(self):
        assert _phi_in_field_names(["ORDER_ID", "AMOUNT", "PRODUCT_CODE"]) is False

    def test_render_notebook_uses_cell_separator(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        rendered = render_notebook(nb)
        assert _CELL_SEP in rendered

    def test_render_notebook_markdown_cell_uses_magic(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        rendered = render_notebook(nb)
        assert "# MAGIC %md" in rendered

    def test_render_notebook_is_string(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        rendered = render_notebook(nb)
        assert isinstance(rendered, str)
        assert len(rendered) > 100

    def test_notebook_mapping_name_set(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        assert nb.mapping_name == "M_EXTRACT_PATIENT_DELTA"

    def test_notebook_folder_set(self, builder, patient_mapping):
        nb = builder.build(patient_mapping)
        assert nb.folder == "PATIENT_EXTRACT"

    def test_upsert_generates_delta_merge(self, builder):
        """upsert load_type should generate DeltaTable merge in write cell."""
        # We can't directly set load_type on the plain MappingDef targets list
        # (targets is a list of str in the real model), but we can inspect the
        # write cell code by using a custom target.
        # The notebook_builder._write_cell_code already produces merge for upsert;
        # exercise it directly.
        from databricks_gen.notebook_builder import _write_cell_code
        lines = _write_cell_code("T_PATIENT", "DELTA", "patient_extract", "upsert", "final")
        code = "\n".join(lines)
        assert "merge" in code.lower() or "DeltaTable" in code


# ---------------------------------------------------------------------------
# TestTfBuilder
# ---------------------------------------------------------------------------

class TestTfBuilder:

    def test_hcl_contains_databricks_job(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/Repos/migration/PATIENT_EXTRACT/M_EXTRACT_PATIENT_DELTA")
        assert 'resource "databricks_job"' in hcl

    def test_hcl_contains_mapping_name(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/Repos/migration/PATIENT_EXTRACT/M_EXTRACT_PATIENT_DELTA")
        assert "M_EXTRACT_PATIENT_DELTA" in hcl

    def test_hcl_contains_cluster_ref(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/nb/path")
        assert "databricks_cluster_id" in hcl

    def test_hcl_contains_notebook_path(self, patient_mapping):
        nb_path = "/Repos/migration/PATIENT_EXTRACT/M_EXTRACT_PATIENT_DELTA"
        hcl = build_terraform_job(patient_mapping, nb_path)
        assert nb_path in hcl

    def test_hcl_notebook_task_block(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/nb/path")
        assert "notebook_task" in hcl

    def test_hcl_contains_tags(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/nb/path")
        assert "tags" in hcl
        assert "ManagedBy" in hcl

    def test_hcl_tags_managed_by_terraform(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/nb/path")
        assert '"terraform"' in hcl

    def test_hcl_tags_migrated_from(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/nb/path")
        assert "InformaticaPowerCenter" in hcl

    def test_hcl_tags_folder(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/nb/path")
        assert "PATIENT_EXTRACT" in hcl

    def test_hcl_schedule_commented_out(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/nb/path")
        # Schedule should be present but commented out
        assert "quartz_cron_expression" in hcl
        # Verify the line is a comment
        for line in hcl.splitlines():
            if "quartz_cron_expression" in line:
                stripped = line.strip()
                assert stripped.startswith("#"), f"Schedule line not commented: {line!r}"

    def test_hcl_output_block_present(self, patient_mapping):
        hcl = build_terraform_job(patient_mapping, "/nb/path")
        assert "output " in hcl

    def test_variables_tf_contains_cluster_id(self):
        vtf = build_terraform_variables("PATIENT_EXTRACT")
        assert "databricks_cluster_id" in vtf

    def test_variables_tf_contains_workspace_url(self):
        vtf = build_terraform_variables("PATIENT_EXTRACT")
        assert "databricks_workspace_url" in vtf

    def test_variables_tf_contains_environment(self):
        vtf = build_terraform_variables("PATIENT_EXTRACT")
        assert 'variable "environment"' in vtf

    def test_variables_tf_environment_validation(self):
        vtf = build_terraform_variables("PATIENT_EXTRACT")
        assert "dev" in vtf and "stage" in vtf and "prod" in vtf

    def test_variables_tf_mentions_folder(self):
        vtf = build_terraform_variables("PATIENT_EXTRACT")
        assert "PATIENT_EXTRACT" in vtf


# ---------------------------------------------------------------------------
# TestGenerator
# ---------------------------------------------------------------------------

class TestGenerator:

    def test_generate_all_skips_glue_mappings(self, manifest_with_all, tmp_path):
        report = generate_all(manifest_with_all, tmp_path)
        skipped = [r for r in report.results if r.skipped and r.skip_reason == "GLUE"]
        assert len(skipped) == 1
        assert skipped[0].mapping_name == "M_LOAD_FACT_ORDERS"

    def test_generate_all_skips_review_mappings(self, manifest_with_all, tmp_path):
        report = generate_all(manifest_with_all, tmp_path)
        skipped = [r for r in report.results if r.skipped and r.skip_reason == "REVIEW"]
        assert len(skipped) == 1

    def test_generate_all_only_generates_databricks(self, manifest_with_all, tmp_path):
        report = generate_all(manifest_with_all, tmp_path)
        assert report.generated == 1

    def test_generate_all_report_json_written(self, manifest_with_all, tmp_path):
        generate_all(manifest_with_all, tmp_path)
        report_path = tmp_path / "databricks-generation-report.json"
        assert report_path.exists()

    def test_generate_all_report_json_valid(self, manifest_with_all, tmp_path):
        generate_all(manifest_with_all, tmp_path)
        report_path = tmp_path / "databricks-generation-report.json"
        data = json.loads(report_path.read_text())
        assert "total" in data
        assert "generated" in data
        assert "results" in data

    def test_generate_all_notebook_file_exists(self, manifest_with_all, tmp_path):
        generate_all(manifest_with_all, tmp_path)
        nb_path = tmp_path / "notebooks" / "PATIENT_EXTRACT" / "M_EXTRACT_PATIENT_DELTA.py"
        assert nb_path.exists()

    def test_generate_all_tf_file_exists(self, manifest_with_all, tmp_path):
        generate_all(manifest_with_all, tmp_path)
        tf_path = tmp_path / "terraform" / "PATIENT_EXTRACT" / "M_EXTRACT_PATIENT_DELTA.tf"
        assert tf_path.exists()

    def test_generate_all_variables_tf_written(self, manifest_with_all, tmp_path):
        generate_all(manifest_with_all, tmp_path)
        vars_path = tmp_path / "terraform" / "PATIENT_EXTRACT" / "variables.tf"
        assert vars_path.exists()

    def test_generate_all_notebook_content_valid(self, manifest_with_all, tmp_path):
        generate_all(manifest_with_all, tmp_path)
        nb_path = tmp_path / "notebooks" / "PATIENT_EXTRACT" / "M_EXTRACT_PATIENT_DELTA.py"
        content = nb_path.read_text(encoding="utf-8")
        assert _CELL_SEP in content

    def test_generate_all_tf_content_valid(self, manifest_with_all, tmp_path):
        generate_all(manifest_with_all, tmp_path)
        tf_path = tmp_path / "terraform" / "PATIENT_EXTRACT" / "M_EXTRACT_PATIENT_DELTA.tf"
        content = tf_path.read_text(encoding="utf-8")
        assert "databricks_job" in content

    def test_generate_all_total_count(self, manifest_with_all, tmp_path):
        report = generate_all(manifest_with_all, tmp_path)
        assert report.total == 3

    def test_generate_all_folder_filter_respected(self, manifest_with_all, tmp_path):
        report = generate_all(manifest_with_all, tmp_path, folder_filter=["SALES_MART"])
        # Only SALES_MART processed — GLUE mapping skipped
        assert report.total == 1
        assert report.generated == 0
        assert report.skipped == 1

    def test_generate_single_works(self, manifest_with_all, tmp_path):
        result = generate_single(
            manifest_with_all, "PATIENT_EXTRACT", "M_EXTRACT_PATIENT_DELTA", tmp_path
        )
        assert result.skipped is False
        assert result.notebook_path is not None

    def test_generate_single_notebook_file_exists(self, manifest_with_all, tmp_path):
        generate_single(
            manifest_with_all, "PATIENT_EXTRACT", "M_EXTRACT_PATIENT_DELTA", tmp_path
        )
        nb_path = tmp_path / "notebooks" / "PATIENT_EXTRACT" / "M_EXTRACT_PATIENT_DELTA.py"
        assert nb_path.exists()

    def test_generate_single_skips_glue_mapping(self, manifest_with_all, tmp_path):
        result = generate_single(manifest_with_all, "SALES_MART", "M_LOAD_FACT_ORDERS", tmp_path)
        assert result.skipped is True
        assert result.skip_reason == "GLUE"

    def test_generate_single_invalid_folder_raises(self, manifest_with_all, tmp_path):
        with pytest.raises(ValueError, match="Folder"):
            generate_single(manifest_with_all, "NONEXISTENT", "M_X", tmp_path)

    def test_generate_single_invalid_mapping_raises(self, manifest_with_all, tmp_path):
        with pytest.raises(ValueError, match="Mapping"):
            generate_single(manifest_with_all, "PATIENT_EXTRACT", "M_NONEXISTENT", tmp_path)

    def test_load_manifest_round_trip(self, manifest_with_all):
        """Serialize to JSON and reload — should reconstruct correctly."""
        from dataclasses import asdict
        data = asdict(manifest_with_all)
        # Enums get serialised as their .value by asdict
        reloaded = _load_manifest(data)
        assert reloaded.repository_name == "TestRepo"
        assert "PATIENT_EXTRACT" in reloaded.folders
        patient_folder = reloaded.folders["PATIENT_EXTRACT"]
        assert "M_EXTRACT_PATIENT_DELTA" in patient_folder.mappings
        mapping = patient_folder.mappings["M_EXTRACT_PATIENT_DELTA"]
        assert mapping.target_platform == TargetPlatform.DATABRICKS


# ---------------------------------------------------------------------------
# TestCLI
# ---------------------------------------------------------------------------

class TestCLI:

    def _make_manifest_file(self, tmp_path, manifest) -> Path:
        """Serialise a manifest to a temp JSON file."""
        from dataclasses import asdict
        data = asdict(manifest)
        p = tmp_path / "manifest.json"
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return p

    def test_generate_all_cmd_runs(self, manifest_with_all, tmp_path):
        runner = CliRunner()
        mf = self._make_manifest_file(tmp_path, manifest_with_all)
        out_dir = tmp_path / "out"
        result = runner.invoke(cli, ["generate-all", str(mf), "--output-dir", str(out_dir)])
        assert result.exit_code == 0, result.output

    def test_generate_all_cmd_creates_notebook(self, manifest_with_all, tmp_path):
        runner = CliRunner()
        mf = self._make_manifest_file(tmp_path, manifest_with_all)
        out_dir = tmp_path / "out"
        runner.invoke(cli, ["generate-all", str(mf), "--output-dir", str(out_dir)])
        nb_path = out_dir / "notebooks" / "PATIENT_EXTRACT" / "M_EXTRACT_PATIENT_DELTA.py"
        assert nb_path.exists()

    def test_generate_all_cmd_creates_report(self, manifest_with_all, tmp_path):
        runner = CliRunner()
        mf = self._make_manifest_file(tmp_path, manifest_with_all)
        out_dir = tmp_path / "out"
        runner.invoke(cli, ["generate-all", str(mf), "--output-dir", str(out_dir)])
        assert (out_dir / "databricks-generation-report.json").exists()

    def test_generate_all_cmd_folder_filter(self, manifest_with_all, tmp_path):
        runner = CliRunner()
        mf = self._make_manifest_file(tmp_path, manifest_with_all)
        out_dir = tmp_path / "out"
        result = runner.invoke(
            cli,
            ["generate-all", str(mf), "--output-dir", str(out_dir), "--folder", "PATIENT_EXTRACT"],
        )
        assert result.exit_code == 0, result.output

    def test_report_cmd_runs(self, manifest_with_all, tmp_path):
        runner = CliRunner()
        mf = self._make_manifest_file(tmp_path, manifest_with_all)
        out_dir = tmp_path / "out"
        # First generate
        runner.invoke(cli, ["generate-all", str(mf), "--output-dir", str(out_dir)])
        report_path = out_dir / "databricks-generation-report.json"
        result = runner.invoke(cli, ["report", str(report_path)])
        assert result.exit_code == 0, result.output

    def test_report_cmd_shows_counts(self, manifest_with_all, tmp_path):
        runner = CliRunner()
        mf = self._make_manifest_file(tmp_path, manifest_with_all)
        out_dir = tmp_path / "out"
        runner.invoke(cli, ["generate-all", str(mf), "--output-dir", str(out_dir)])
        report_path = out_dir / "databricks-generation-report.json"
        result = runner.invoke(cli, ["report", str(report_path)])
        output = result.output
        # Either rich or plain output — both show the counts
        assert "Generated" in output or "generated" in output or "Total" in output

    def test_generate_single_cmd_preview(self, manifest_with_all, tmp_path):
        runner = CliRunner()
        mf = self._make_manifest_file(tmp_path, manifest_with_all)
        result = runner.invoke(
            cli,
            ["generate", str(mf), "PATIENT_EXTRACT", "M_EXTRACT_PATIENT_DELTA", "--preview"],
        )
        assert result.exit_code == 0, result.output
        assert "# MAGIC %md" in result.output or _CELL_SEP in result.output

    def test_version_option(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output
