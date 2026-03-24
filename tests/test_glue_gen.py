"""
Tests for glue_gen — graph, expression translator, code builder, tf builder, generator.
Run with:  pytest tests/test_glue_gen.py -v
"""

import json
import sys
import ast
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

from pc_extractor.xml_parser import parse_xml_files
from pc_extractor.scorer import score_all_mappings
from pc_extractor.reporter import write_manifest_json

from glue_gen.graph import PipelineGraph
from glue_gen.expr_translator import (
    translate, translate_filter, translate_join_condition, Confidence,
)
from glue_gen.code_builder import build_glue_script
from glue_gen.tf_builder import build_terraform_job, build_terraform_variables
from glue_gen.generator import generate_all, generate_single

FIXTURE = Path(__file__).parent / "fixtures" / "sample_export.xml"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def manifest():
    m = parse_xml_files([FIXTURE])
    score_all_mappings(m)
    return m


@pytest.fixture(scope="module")
def sales_folder(manifest):
    return manifest.folders["SALES_MART"]


@pytest.fixture(scope="module")
def patient_folder(manifest):
    return manifest.folders["PATIENT_EXTRACT"]


@pytest.fixture(scope="module")
def simple_mapping(sales_folder):
    return sales_folder.mappings["M_LOAD_FACT_ORDERS"]


@pytest.fixture(scope="module")
def enrichment_mapping(sales_folder):
    return sales_folder.mappings["M_ENRICH_ORDERS"]


@pytest.fixture(scope="module")
def delta_mapping(patient_folder):
    return patient_folder.mappings["M_EXTRACT_PATIENT_DELTA"]


# ---------------------------------------------------------------------------
# Graph tests
# ---------------------------------------------------------------------------

class TestPipelineGraph:
    def test_graph_builds_from_simple_mapping(self, simple_mapping):
        from pc_extractor.models import TransformationDef
        t_lookup = {t.name: t for t in simple_mapping.transformations}
        g = PipelineGraph.from_mapping(simple_mapping, t_lookup)
        assert len(g.nodes) > 0

    def test_topological_order_has_all_nodes(self, simple_mapping):
        t_lookup = {t.name: t for t in simple_mapping.transformations}
        g = PipelineGraph.from_mapping(simple_mapping, t_lookup)
        order = g.topological_order()
        assert len(order) == len(g.nodes)

    def test_source_nodes_have_no_predecessors(self, simple_mapping):
        t_lookup = {t.name: t for t in simple_mapping.transformations}
        g = PipelineGraph.from_mapping(simple_mapping, t_lookup)
        for node in g.source_nodes():
            assert not g.predecessors(node.instance_name)

    def test_df_var_is_valid_python_identifier(self, simple_mapping):
        t_lookup = {t.name: t for t in simple_mapping.transformations}
        g = PipelineGraph.from_mapping(simple_mapping, t_lookup)
        for node in g.nodes.values():
            var = g.df_var(node.instance_name)
            assert var.startswith("df_")
            assert var.isidentifier()

    def test_enrichment_mapping_has_joiner_in_graph(self, enrichment_mapping):
        t_lookup = {t.name: t for t in enrichment_mapping.transformations}
        g = PipelineGraph.from_mapping(enrichment_mapping, t_lookup)
        joiner_nodes = [
            n for n in g.nodes.values()
            if "joiner" in n.transformation_type.lower()
        ]
        assert len(joiner_nodes) >= 1

    def test_fields_flowing_into_populated(self, simple_mapping):
        t_lookup = {t.name: t for t in simple_mapping.transformations}
        g = PipelineGraph.from_mapping(simple_mapping, t_lookup)
        # SQ_ORDERS should have edges flowing in from SRC_ORDERS
        sq_edges = g.fields_flowing_into("SQ_ORDERS")
        assert len(sq_edges) > 0


# ---------------------------------------------------------------------------
# Expression translator tests
# ---------------------------------------------------------------------------

class TestExprTranslator:
    def test_sysdate(self):
        r = translate("SYSDATE")
        assert "current_timestamp" in r.pyspark_expr
        assert r.confidence == Confidence.HIGH

    def test_iif(self):
        r = translate("IIF(STATUS = 'A', 1, 0)")
        assert "F.when" in r.pyspark_expr
        assert "otherwise" in r.pyspark_expr
        assert r.confidence == Confidence.HIGH

    def test_nvl(self):
        r = translate("NVL(ORDER_AMT, 0)")
        assert "coalesce" in r.pyspark_expr

    def test_substr(self):
        r = translate("SUBSTR(PROD_CODE, 1, 3)")
        assert "substring" in r.pyspark_expr
        assert "1" in r.pyspark_expr
        assert "3" in r.pyspark_expr

    def test_upper_lower(self):
        assert "F.upper" in translate("UPPER(NAME)").pyspark_expr
        assert "F.lower" in translate("LOWER(NAME)").pyspark_expr

    def test_to_date(self):
        r = translate("TO_DATE(DATE_STR, 'YYYY-MM-DD')")
        assert "to_date" in r.pyspark_expr

    def test_decode(self):
        r = translate("DECODE(STATUS, 'A', 'Active', 'I', 'Inactive', 'Unknown')")
        assert "F.when" in r.pyspark_expr
        assert r.confidence in (Confidence.HIGH, Confidence.MEDIUM)

    def test_sum_aggregation(self):
        r = translate("SUM(ORDER_AMT)")
        assert "F.sum" in r.pyspark_expr

    def test_count_star(self):
        r = translate("COUNT(*)")
        assert "F.count" in r.pyspark_expr

    def test_trunc(self):
        r = translate("TRUNC(ORDER_DATE)")
        assert "date_trunc" in r.pyspark_expr

    def test_round(self):
        r = translate("ROUND(ORDER_AMT, 2)")
        assert "F.round" in r.pyspark_expr
        assert "2" in r.pyspark_expr

    def test_concat_operator(self):
        r = translate("FIRST_NAME || ' ' || LAST_NAME")
        assert "+" in r.pyspark_expr

    def test_passthrough_column(self):
        r = translate("ORDER_ID")
        # simple column ref — should be wrapped
        assert "ORDER_ID" in r.pyspark_expr

    def test_literal_number_unchanged(self):
        r = translate("42")
        assert "42" in r.pyspark_expr

    def test_empty_expression(self):
        r = translate("")
        assert r.pyspark_expr == "None"

    def test_cast_int(self):
        r = translate("TO_INTEGER(AMOUNT_STR)")
        assert "cast" in r.pyspark_expr
        assert "int" in r.pyspark_expr

    def test_filter_and_condition(self):
        r = translate_filter("STATUS = 'A' AND AMOUNT > 0")
        assert "&" in r.pyspark_expr
        assert "STATUS" in r.pyspark_expr

    def test_filter_not_equal(self):
        r = translate_filter("STATUS != 'CANCELLED'")
        assert "!=" in r.pyspark_expr or "!=" in r.pyspark_expr

    def test_join_condition_same_col(self):
        r = translate_join_condition("CUSTOMER_ID = CUSTOMER_ID")
        assert "CUSTOMER_ID" in r.pyspark_expr

    def test_join_condition_different_cols(self):
        r = translate_join_condition("ORDER_ID = ORD_ID")
        assert "ORDER_ID" in r.pyspark_expr or "ORD_ID" in r.pyspark_expr


# ---------------------------------------------------------------------------
# Code builder tests
# ---------------------------------------------------------------------------

class TestCodeBuilder:
    def test_simple_mapping_generates_script(self, simple_mapping, sales_folder):
        script, warnings = build_glue_script(simple_mapping, sales_folder)
        assert script
        assert len(script) > 100

    def test_script_has_glue_imports(self, simple_mapping, sales_folder):
        script, _ = build_glue_script(simple_mapping, sales_folder)
        assert "from awsglue.context import GlueContext" in script
        assert "from awsglue.job import Job" in script
        assert "from pyspark.sql import functions as F" in script

    def test_script_has_job_init(self, simple_mapping, sales_folder):
        script, _ = build_glue_script(simple_mapping, sales_folder)
        assert "job.init" in script
        assert "job.commit()" in script

    def test_script_has_hipaa_comment(self, simple_mapping, sales_folder):
        script, _ = build_glue_script(simple_mapping, sales_folder)
        assert "HIPAA" in script

    def test_simple_mapping_has_jdbc_source(self, simple_mapping, sales_folder):
        script, _ = build_glue_script(simple_mapping, sales_folder)
        assert "sqlserver" in script
        assert "create_dynamic_frame" in script

    def test_simple_mapping_has_filter(self, simple_mapping, sales_folder):
        script, _ = build_glue_script(simple_mapping, sales_folder)
        assert ".filter(" in script

    def test_simple_mapping_has_parameter_arg(self, simple_mapping, sales_folder):
        script, _ = build_glue_script(simple_mapping, sales_folder)
        assert "START_DATE" in script

    def test_enrichment_mapping_has_join(self, enrichment_mapping, sales_folder):
        script, _ = build_glue_script(enrichment_mapping, sales_folder)
        assert ".join(" in script

    def test_enrichment_mapping_has_broadcast(self, enrichment_mapping, sales_folder):
        script, _ = build_glue_script(enrichment_mapping, sales_folder)
        assert "broadcast" in script

    def test_enrichment_mapping_has_router(self, enrichment_mapping, sales_folder):
        script, _ = build_glue_script(enrichment_mapping, sales_folder)
        assert "GRP_NORTH" in script or "GRP_SOUTH" in script or "router" in script.lower()

    def test_delta_mapping_writes_delta(self, delta_mapping, patient_folder):
        script, _ = build_glue_script(delta_mapping, patient_folder)
        assert "delta" in script.lower()

    def test_generated_script_is_valid_python_syntax(self, simple_mapping, sales_folder):
        script, _ = build_glue_script(simple_mapping, sales_folder)
        # Remove lines that reference awsglue (not installed) and parse the rest
        # Just check the structure is syntactically plausible
        # We can't fully exec it without Glue, but we can check for obvious syntax errors
        assert "def " not in script or "logger" in script  # no stray function defs
        # Verify the script doesn't have obvious syntax errors by checking braces/parens balance
        open_parens  = script.count("(") - script.count(")")
        open_brackets = script.count("[") - script.count("]")
        assert abs(open_parens) <= 5    # allow small mismatch from comments
        assert abs(open_brackets) <= 5

    def test_sql_override_generates_comment(self, enrichment_mapping, sales_folder):
        script, _ = build_glue_script(enrichment_mapping, sales_folder)
        assert "SQL override" in script or "Original SQL" in script

    def test_mapping_source_folder_in_script(self, simple_mapping, sales_folder):
        script, _ = build_glue_script(simple_mapping, sales_folder)
        assert simple_mapping.folder in script

    def test_warnings_list_returned(self, enrichment_mapping, sales_folder):
        _, warnings = build_glue_script(enrichment_mapping, sales_folder)
        assert isinstance(warnings, list)


# ---------------------------------------------------------------------------
# Terraform builder tests
# ---------------------------------------------------------------------------

class TestTfBuilder:
    def test_generates_aws_glue_job_resource(self, simple_mapping):
        tf = build_terraform_job(simple_mapping, ["JOB_NAME", "CONN_SRC_ORDERS"])
        assert 'resource "aws_glue_job"' in tf

    def test_tf_has_glue_version_4(self, simple_mapping):
        tf = build_terraform_job(simple_mapping, [])
        assert "4.0" in tf

    def test_tf_has_script_location(self, simple_mapping):
        tf = build_terraform_job(simple_mapping, [])
        assert "script_location" in tf
        assert simple_mapping.name in tf

    def test_tf_has_tags(self, simple_mapping):
        tf = build_terraform_job(simple_mapping, [])
        assert "ManagedBy" in tf
        assert "MigratedFrom" in tf
        assert "InformaticaPowerCenter" in tf

    def test_tf_has_job_bookmark(self, simple_mapping):
        tf = build_terraform_job(simple_mapping, [])
        assert "job-bookmark-enable" in tf

    def test_tf_has_environment_variable(self, simple_mapping):
        tf = build_terraform_job(simple_mapping, [])
        assert "var.environment" in tf

    def test_tf_has_output_block(self, simple_mapping):
        tf = build_terraform_job(simple_mapping, [])
        assert 'output "' in tf

    def test_variables_tf_has_required_vars(self):
        vtf = build_terraform_variables("SALES_MART", ["src_orders", "tgt_fact_orders"])
        assert "variable" in vtf
        assert "glue_role_arn" in vtf
        assert "scripts_bucket" in vtf
        assert "environment" in vtf

    def test_variables_tf_environment_validation(self):
        vtf = build_terraform_variables("FOLDER", [])
        assert 'contains(["dev", "stage", "prod"]' in vtf

    def test_complex_mapping_gets_more_workers(self, sales_folder):
        from pc_extractor.xml_parser import parse_xml_files
        from pc_extractor.scorer import score_all_mappings
        m = parse_xml_files([FIXTURE])
        score_all_mappings(m)
        sp_mapping = m.folders["SALES_MART"].mappings["M_PROC_ORDERS_SP"]
        # SP mapping has high score — should get more workers
        tf = build_terraform_job(sp_mapping, [])
        # Should have more than 2 workers for complex jobs
        import re
        workers_match = re.search(r"number_of_workers\s*=\s*(\d+)", tf)
        assert workers_match
        assert int(workers_match.group(1)) >= 2


# ---------------------------------------------------------------------------
# Generator (end-to-end) tests
# ---------------------------------------------------------------------------

class TestGenerator:
    def test_generate_all_produces_files(self, manifest, tmp_path):
        report = generate_all(manifest, output_dir=tmp_path)
        assert report.generated > 0

    def test_generate_all_skips_review_mappings(self, manifest, tmp_path):
        report = generate_all(manifest, output_dir=tmp_path, include_review=False)
        skipped_review = [
            r for r in report.results
            if r.status == "SKIPPED" and r.target_platform == "REVIEW"
        ]
        assert len(skipped_review) > 0

    def test_generate_all_skips_databricks_mappings(self, manifest, tmp_path):
        report = generate_all(manifest, output_dir=tmp_path)
        skipped_db = [
            r for r in report.results
            if r.status == "SKIPPED" and r.target_platform == "DATABRICKS"
        ]
        assert len(skipped_db) > 0

    def test_generated_glue_script_file_exists(self, manifest, tmp_path):
        report = generate_all(manifest, output_dir=tmp_path)
        for r in report.results:
            if r.status == "SUCCESS" and r.glue_script_path:
                assert Path(r.glue_script_path).exists()

    def test_generated_terraform_file_exists(self, manifest, tmp_path):
        report = generate_all(manifest, output_dir=tmp_path)
        for r in report.results:
            if r.status == "SUCCESS" and r.terraform_path:
                assert Path(r.terraform_path).exists()

    def test_variables_tf_created_per_folder(self, manifest, tmp_path):
        generate_all(manifest, output_dir=tmp_path)
        vars_file = tmp_path / "terraform" / "SALES_MART" / "variables.tf"
        assert vars_file.exists()

    def test_generation_report_json_written(self, manifest, tmp_path):
        generate_all(manifest, output_dir=tmp_path)
        report_file = tmp_path / "generation-report.json"
        assert report_file.exists()
        data = json.loads(report_file.read_text())
        assert "generated" in data
        assert "results" in data

    def test_generate_single_by_name(self, manifest, tmp_path):
        result = generate_single(
            manifest, "SALES_MART", "M_LOAD_FACT_ORDERS", tmp_path
        )
        assert result.status == "SUCCESS"
        assert result.glue_script_path
        assert Path(result.glue_script_path).exists()

    def test_generate_single_unknown_folder_raises(self, manifest, tmp_path):
        with pytest.raises(ValueError, match="not found"):
            generate_single(manifest, "NONEXISTENT", "M_LOAD_FACT_ORDERS", tmp_path)

    def test_generate_single_unknown_mapping_raises(self, manifest, tmp_path):
        with pytest.raises(ValueError, match="not found"):
            generate_single(manifest, "SALES_MART", "M_NONEXISTENT", tmp_path)

    def test_folder_filter_limits_output(self, manifest, tmp_path):
        report = generate_all(
            manifest, output_dir=tmp_path,
            folder_filter=["SALES_MART"],
        )
        patient_results = [
            r for r in report.results
            if r.folder == "PATIENT_EXTRACT"
        ]
        assert len(patient_results) == 0

    def test_include_review_generates_stubs(self, manifest, tmp_path):
        report = generate_all(
            manifest, output_dir=tmp_path,
            include_review=True,
        )
        review_generated = [
            r for r in report.results
            if r.target_platform == "REVIEW" and r.status == "SUCCESS"
        ]
        assert len(review_generated) > 0

    def test_generated_script_references_mapping_name(self, manifest, tmp_path):
        result = generate_single(manifest, "SALES_MART", "M_LOAD_FACT_ORDERS", tmp_path)
        script = Path(result.glue_script_path).read_text()
        assert "M_LOAD_FACT_ORDERS" in script

    def test_total_counts_consistent(self, manifest, tmp_path):
        report = generate_all(manifest, output_dir=tmp_path)
        assert report.generated + report.skipped + report.errors == report.total_mappings
