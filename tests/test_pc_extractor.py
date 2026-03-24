"""
Tests for pc_extractor — parser, scorer, reporter.
Run with:  pytest tests/ -v
"""

import json
import sys
from pathlib import Path

import pytest

# Make tools/ importable
sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

from pc_extractor.xml_parser import parse_xml_files, parse_xml_file
from pc_extractor.scorer import score_all_mappings, score_mapping
from pc_extractor.reporter import write_manifest_json, write_backlog_csv, write_summary_txt
from pc_extractor.models import TargetPlatform, TransformationType

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


# ---------------------------------------------------------------------------
# Parser — repository level
# ---------------------------------------------------------------------------

class TestRepositoryParsing:
    def test_repository_name(self, manifest):
        assert manifest.repository_name == "DEV_REPO"

    def test_two_folders(self, manifest):
        assert len(manifest.folders) == 2
        assert "SALES_MART" in manifest.folders
        assert "PATIENT_EXTRACT" in manifest.folders

    def test_extracted_at_is_set(self, manifest):
        assert manifest.extracted_at


# ---------------------------------------------------------------------------
# Parser — sources
# ---------------------------------------------------------------------------

class TestSourceParsing:
    def test_sales_folder_has_three_sources(self, sales_folder):
        assert len(sales_folder.sources) == 3

    def test_sql_server_source(self, sales_folder):
        src = sales_folder.sources["SRC_ORDERS"]
        assert src.db_type == "Microsoft SQL Server"
        assert src.db_name == "SalesDB"
        assert src.owner == "dbo"
        assert len(src.fields) == 5

    def test_flat_file_source_detected(self, sales_folder):
        src = sales_folder.sources["SRC_PRODUCT_FILE"]
        assert src.db_type == "FLAT FILE"
        assert src.is_fixed_width is True
        assert len(src.fields) == 3

    def test_source_field_key_type(self, sales_folder):
        src = sales_folder.sources["SRC_ORDERS"]
        order_id_field = next(f for f in src.fields if f.name == "ORDER_ID")
        assert order_id_field.key_type == "PRIMARY KEY"
        assert order_id_field.nullable is False

    def test_patient_source_parsed(self, patient_folder):
        src = patient_folder.sources["SRC_PATIENTS"]
        assert src.db_type == "Microsoft SQL Server"
        assert len(src.fields) == 5


# ---------------------------------------------------------------------------
# Parser — targets
# ---------------------------------------------------------------------------

class TestTargetParsing:
    def test_delta_target_type(self, patient_folder):
        tgt = patient_folder.targets["TGT_PATIENT_DELTA"]
        assert tgt.db_type == "DELTA"

    def test_sql_server_target(self, sales_folder):
        tgt = sales_folder.targets["TGT_FACT_ORDERS"]
        assert tgt.db_type == "Microsoft SQL Server"
        assert len(tgt.fields) == 7


# ---------------------------------------------------------------------------
# Parser — mappings
# ---------------------------------------------------------------------------

class TestMappingParsing:
    def test_sales_folder_has_three_mappings(self, sales_folder):
        assert len(sales_folder.mappings) == 3

    def test_simple_mapping_parsed(self, sales_folder):
        m = sales_folder.mappings["M_LOAD_FACT_ORDERS"]
        assert m.is_valid is True
        assert len(m.sources) == 1
        assert len(m.targets) == 1
        assert len(m.transformations) == 3
        assert len(m.connectors) > 0

    def test_mapping_instances_populated(self, sales_folder):
        m = sales_folder.mappings["M_LOAD_FACT_ORDERS"]
        assert "SRC_ORDERS" in m.sources
        assert "TGT_FACT_ORDERS" in m.targets

    def test_mapping_parameter_detected(self, sales_folder):
        m = sales_folder.mappings["M_LOAD_FACT_ORDERS"]
        params = [v for v in m.variables if v.is_param]
        assert len(params) == 1
        assert params[0].name == "$$START_DATE"

    def test_enrichment_mapping_multi_source(self, sales_folder):
        m = sales_folder.mappings["M_ENRICH_ORDERS"]
        assert m.flags.multi_source is True
        assert len(m.sources) == 2

    def test_enrichment_mapping_has_joiner(self, sales_folder):
        m = sales_folder.mappings["M_ENRICH_ORDERS"]
        assert m.flags.has_joiner is True

    def test_enrichment_mapping_has_lookup(self, sales_folder):
        m = sales_folder.mappings["M_ENRICH_ORDERS"]
        assert m.flags.has_lookup is True

    def test_enrichment_mapping_has_router(self, sales_folder):
        m = sales_folder.mappings["M_ENRICH_ORDERS"]
        assert m.flags.has_router is True

    def test_enrichment_mapping_sql_override(self, sales_folder):
        m = sales_folder.mappings["M_ENRICH_ORDERS"]
        assert m.flags.has_sql_override is True

    def test_stored_proc_mapping_detected(self, sales_folder):
        m = sales_folder.mappings["M_PROC_ORDERS_SP"]
        assert m.flags.has_stored_proc is True
        sp = next(
            t for t in m.transformations
            if t.type == TransformationType.STORED_PROCEDURE
        )
        assert sp.stored_proc_name == "dbo.usp_CalculateTax"

    def test_router_groups_parsed(self, sales_folder):
        m = sales_folder.mappings["M_ENRICH_ORDERS"]
        router = next(
            t for t in m.transformations if t.type == TransformationType.ROUTER
        )
        assert len(router.router_groups) == 3
        group_names = {g.name for g in router.router_groups}
        assert "GRP_NORTH" in group_names
        assert "DEFAULT1" in group_names

    def test_lookup_condition_parsed(self, sales_folder):
        m = sales_folder.mappings["M_ENRICH_ORDERS"]
        lkp = next(
            t for t in m.transformations if t.type == TransformationType.LOOKUP
        )
        assert lkp.lookup_condition is not None
        assert "CUSTOMER_ID" in lkp.lookup_condition

    def test_joiner_type_parsed(self, sales_folder):
        m = sales_folder.mappings["M_ENRICH_ORDERS"]
        jnr = next(
            t for t in m.transformations if t.type == TransformationType.JOINER
        )
        assert jnr.join_type == "Normal Join"


# ---------------------------------------------------------------------------
# Parser — workflows
# ---------------------------------------------------------------------------

class TestWorkflowParsing:
    def test_sales_workflow_parsed(self, sales_folder):
        wf = sales_folder.workflows["WF_SALES_MART_DAILY"]
        assert wf.is_enabled is True
        assert wf.server_name == "INFA_SERVER_01"

    def test_workflow_has_three_session_tasks(self, sales_folder):
        wf = sales_folder.workflows["WF_SALES_MART_DAILY"]
        sessions = [t for t in wf.tasks if t.task_type.upper() == "SESSION"]
        assert len(sessions) == 3

    def test_workflow_mapping_refs(self, sales_folder):
        wf = sales_folder.workflows["WF_SALES_MART_DAILY"]
        assert "M_LOAD_FACT_ORDERS" in wf.mapping_refs
        assert "M_ENRICH_ORDERS" in wf.mapping_refs
        assert "M_PROC_ORDERS_SP" in wf.mapping_refs

    def test_workflow_links(self, sales_folder):
        wf = sales_folder.workflows["WF_SALES_MART_DAILY"]
        assert len(wf.links) == 4

    def test_workflow_scheduler(self, sales_folder):
        wf = sales_folder.workflows["WF_SALES_MART_DAILY"]
        assert wf.scheduler.schedule_type == "CUSTOMIZED"
        assert wf.scheduler.start_time == "02:00:00"


# ---------------------------------------------------------------------------
# Scorer
# ---------------------------------------------------------------------------

class TestScorer:
    def test_simple_mapping_low_score(self, sales_folder):
        m = sales_folder.mappings["M_LOAD_FACT_ORDERS"]
        assert m.complexity_score is not None
        assert m.complexity_score <= 4

    def test_simple_mapping_routes_to_glue(self, sales_folder):
        m = sales_folder.mappings["M_LOAD_FACT_ORDERS"]
        assert m.target_platform == TargetPlatform.GLUE

    def test_enrichment_mapping_moderate_score(self, sales_folder):
        m = sales_folder.mappings["M_ENRICH_ORDERS"]
        assert m.complexity_score is not None
        assert 3 <= m.complexity_score <= 8

    def test_stored_proc_mapping_high_score(self, sales_folder):
        m = sales_folder.mappings["M_PROC_ORDERS_SP"]
        assert m.complexity_score is not None
        assert m.complexity_score >= 5

    def test_stored_proc_mapping_routes_to_review(self, sales_folder):
        m = sales_folder.mappings["M_PROC_ORDERS_SP"]
        assert m.target_platform == TargetPlatform.REVIEW

    def test_delta_target_routes_to_databricks(self, patient_folder):
        m = patient_folder.mappings["M_EXTRACT_PATIENT_DELTA"]
        assert m.target_platform == TargetPlatform.DATABRICKS

    def test_parameter_flag_adds_to_score(self, sales_folder):
        m = sales_folder.mappings["M_LOAD_FACT_ORDERS"]
        assert m.flags.has_parameter_vars is True
        assert any("parameter" in r.lower() for r in m.complexity_reasons)

    def test_review_mapping_has_review_notes(self, sales_folder):
        m = sales_folder.mappings["M_PROC_ORDERS_SP"]
        assert len(m.review_notes) > 0

    def test_score_capped_at_10(self, manifest):
        for folder in manifest.folders.values():
            for m in folder.mappings.values():
                assert m.complexity_score <= 10

    def test_summary_counts_match(self, manifest):
        s = manifest.summary
        total_routed = s.routed_glue + s.routed_databricks + s.routed_review
        assert total_routed == s.total_mappings


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------

class TestReporter:
    def test_write_manifest_json(self, manifest, tmp_path):
        out = write_manifest_json(manifest, tmp_path)
        assert out.exists()
        data = json.loads(out.read_text())
        assert data["repository_name"] == "DEV_REPO"
        assert "SALES_MART" in data["folders"]

    def test_manifest_json_has_summary(self, manifest, tmp_path):
        out = write_manifest_json(manifest, tmp_path)
        data = json.loads(out.read_text())
        s = data["summary"]
        assert s["total_mappings"] == 4
        assert s["total_workflows"] == 2

    def test_write_backlog_csv(self, manifest, tmp_path):
        out = write_backlog_csv(manifest, tmp_path)
        assert out.exists()
        import csv
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert len(rows) == 4   # 4 mappings total

    def test_backlog_csv_has_required_columns(self, manifest, tmp_path):
        out = write_backlog_csv(manifest, tmp_path)
        import csv
        reader = csv.DictReader(out.open(encoding="utf-8"))
        cols = set(reader.fieldnames or [])
        for required in ("mapping_name", "complexity_score", "target_platform",
                         "flag_stored_proc", "sprint_estimate_days"):
            assert required in cols, f"Missing column: {required}"

    def test_backlog_workflow_ref_populated(self, manifest, tmp_path):
        out = write_backlog_csv(manifest, tmp_path)
        import csv
        rows = {r["mapping_name"]: r for r in csv.DictReader(out.open(encoding="utf-8"))}
        assert "WF_SALES_MART_DAILY" in rows["M_LOAD_FACT_ORDERS"]["referenced_by_workflows"]

    def test_write_summary_txt(self, manifest, tmp_path):
        out = write_summary_txt(manifest, tmp_path)
        assert out.exists()
        content = out.read_text()
        assert "DEV_REPO" in content
        assert "AWS Glue" in content
        assert "Databricks" in content

    def test_sprint_estimate_positive(self, manifest, tmp_path):
        out = write_backlog_csv(manifest, tmp_path)
        import csv
        for row in csv.DictReader(out.open(encoding="utf-8")):
            assert float(row["sprint_estimate_days"]) > 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_invalid_xml_raises(self, tmp_path):
        bad = tmp_path / "bad.xml"
        bad.write_text("<POWERMART><BROKEN>")
        with pytest.raises(ValueError, match="Malformed XML"):
            parse_xml_file(bad)

    def test_wrong_root_tag_raises(self, tmp_path):
        bad = tmp_path / "wrong.xml"
        bad.write_text('<?xml version="1.0"?><NOT_POWERMART/>')
        with pytest.raises(ValueError, match="POWERMART"):
            parse_xml_file(bad)

    def test_empty_folder_list(self, tmp_path):
        empty = tmp_path / "empty.xml"
        empty.write_text(
            '<?xml version="1.0"?>'
            '<POWERMART><REPOSITORY NAME="EMPTY" VERSION="1" CODEPAGE="UTF-8" DATABASETYPE="Oracle">'
            '</REPOSITORY></POWERMART>'
        )
        repo_name, folders = parse_xml_file(empty)
        assert repo_name == "EMPTY"
        assert folders == {}
