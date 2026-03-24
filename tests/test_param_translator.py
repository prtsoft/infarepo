"""
Tests for param_translator — parser, classifier, normalizer, exporter.
Run with:  pytest tests/test_param_translator.py -v
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

from param_translator.parser import parse_prm_text, parse_prm_file
from param_translator.classifier import classify, classify_file
from param_translator.normalizer import normalize_file, normalize_param, _normalize_date, _normalize_date_mask
from param_translator.exporter import export_all, write_full_params_json, write_glue_params_json
from param_translator.models import ParamType, SectionType

FIXTURE = Path(__file__).parent / "fixtures" / "sample.prm"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def prm():
    p = parse_prm_file(FIXTURE)
    classify_file(p)
    normalize_file(p)
    return p


@pytest.fixture(scope="module")
def global_section(prm):
    return next(s for s in prm.sections if s.section_type == SectionType.GLOBAL)


@pytest.fixture(scope="module")
def workflow_section(prm):
    return next(s for s in prm.sections if s.section_type == SectionType.WORKFLOW
                and s.workflow == "WF_SALES_MART_DAILY")


@pytest.fixture(scope="module")
def load_session(prm):
    return next(s for s in prm.sections
                if s.task == "s_M_LOAD_FACT_ORDERS")


@pytest.fixture(scope="module")
def enrich_session(prm):
    return next(s for s in prm.sections
                if s.task == "s_M_ENRICH_ORDERS")


@pytest.fixture(scope="module")
def sp_session(prm):
    return next(s for s in prm.sections
                if s.task == "s_M_PROC_ORDERS_SP")


@pytest.fixture(scope="module")
def patient_workflow(prm):
    return next(s for s in prm.sections
                if s.section_type == SectionType.WORKFLOW
                and s.workflow == "WF_PATIENT_EXTRACT_NIGHTLY")


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

class TestParser:
    def test_parses_all_sections(self, prm):
        assert len(prm.sections) == 7

    def test_global_section_detected(self, prm):
        globals_ = [s for s in prm.sections if s.section_type == SectionType.GLOBAL]
        assert len(globals_) == 1

    def test_workflow_sections_detected(self, prm):
        workflows = [s for s in prm.sections if s.section_type == SectionType.WORKFLOW]
        assert len(workflows) == 2

    def test_session_sections_detected(self, prm):
        sessions = [s for s in prm.sections if s.section_type == SectionType.SESSION]
        assert len(sessions) == 4

    def test_section_key_format(self, load_session):
        assert "SALES_MART" in load_session.key
        assert "WF_SALES_MART_DAILY" in load_session.key
        assert "s_M_LOAD_FACT_ORDERS" in load_session.key

    def test_folder_name_parsed(self, load_session):
        assert load_session.folder == "SALES_MART"

    def test_workflow_name_parsed(self, load_session):
        assert load_session.workflow == "WF_SALES_MART_DAILY"

    def test_task_name_parsed(self, load_session):
        assert load_session.task == "s_M_LOAD_FACT_ORDERS"

    def test_param_name_stripped_of_dollars(self, global_section):
        assert "ENV" in global_section.params
        assert "BATCH_ENABLED" in global_section.params

    def test_empty_param_parsed(self, global_section):
        assert "EMPTY_PARAM" in global_section.params
        assert global_section.params["EMPTY_PARAM"].raw_value == ""

    def test_backslash_multiline_joined(self, enrich_session):
        param = enrich_session.params["ENRICH_SQL"]
        # Backslash continuation lines should be joined
        assert "INNER JOIN" in param.raw_value
        assert "WHERE" in param.raw_value

    def test_bare_multiline_joined(self, sp_session):
        param = sp_session.params["SP_FILTER"]
        assert "WHERE" in param.raw_value
        assert "BETWEEN" in param.raw_value

    def test_inline_comment_stripped(self, patient_workflow):
        # PHI_MASKING=Y ; HIPAA comment — value should be just "Y"
        param = patient_workflow.params["PHI_MASKING"]
        assert param.raw_value.strip() == "Y"

    def test_comments_not_parsed_as_params(self, prm):
        # Lines starting with # are comments, not params
        for section in prm.sections:
            for name in section.params:
                assert not name.startswith("#")

    def test_merged_later_sections_win(self, prm):
        # START_DATE appears in both workflow and load_session
        # The session (more specific) should win in merged
        merged = prm.merged
        assert "START_DATE" in merged
        # Session value is 01/15/2024 vs workflow 01/01/2024
        assert "15" in merged["START_DATE"].raw_value

    def test_from_string(self):
        text = "[Global]\n$$FOO=bar\n$$NUM=42\n"
        prm = parse_prm_text(text, "<test>")
        assert len(prm.sections) == 1
        assert "FOO" in prm.sections[0].params
        assert "NUM" in prm.sections[0].params

    def test_empty_file(self):
        prm = parse_prm_text("", "<empty>")
        assert prm.sections == [] or all(not s.params for s in prm.sections)

    def test_no_section_header_params_go_to_global(self):
        text = "$$LOOSE=value\n[FOLDER.WF]\n$$BOUND=other\n"
        prm = parse_prm_text(text)
        globals_ = [s for s in prm.sections if s.section_type == SectionType.GLOBAL]
        assert any("LOOSE" in s.params for s in globals_)


# ---------------------------------------------------------------------------
# Classifier tests
# ---------------------------------------------------------------------------

class TestClassifier:
    # --- DATE ---
    def test_us_date(self):         assert classify("01/15/2024")       == ParamType.DATE
    def test_iso_date(self):        assert classify("2024-01-15")        == ParamType.DATE
    def test_oracle_date(self):     assert classify("15-JAN-2024")       == ParamType.DATE
    def test_compact_date(self):    assert classify("20240115")          == ParamType.DATE
    def test_iso_datetime(self):    assert classify("2024-01-15 14:30:00") == ParamType.DATE
    def test_iso_datetime_t(self):  assert classify("2024-03-23T14:30:00") == ParamType.DATE

    # --- DATE_MASK ---
    def test_us_mask(self):         assert classify("MM/DD/YYYY")         == ParamType.DATE_MASK
    def test_iso_mask(self):        assert classify("YYYY-MM-DD")          == ParamType.DATE_MASK
    def test_oracle_mask(self):     assert classify("DD-MON-YYYY HH24:MI:SS") == ParamType.DATE_MASK
    def test_datetime_mask(self):   assert classify("MM/DD/YYYY HH24:MI:SS") == ParamType.DATE_MASK
    def test_short_mask(self):      assert classify("YYYY-MM-DD")          == ParamType.DATE_MASK

    # --- SQL ---
    def test_select_from_sql(self):
        assert classify("SELECT * FROM TABLE WHERE X = 1") == ParamType.SQL
    def test_filter_condition_sql(self):
        assert classify("STATUS != 'CANCELLED' AND REGION IN ('NORTH','SOUTH')") == ParamType.SQL
    def test_lookup_sql(self):
        assert classify("SELECT CUSTOMER_ID, CUST_NAME FROM dbo.DIM_CUSTOMER WHERE ACTIVE_FLAG = 1") == ParamType.SQL
    def test_multiline_sql(self):
        assert classify("SELECT ORDER_ID FROM dbo.ORDERS\nWHERE STATUS = 'APPROVED'") == ParamType.SQL
    def test_short_word_not_sql(self):
        # "FROM" alone or short strings should not classify as SQL
        assert classify("FROM_DATE") != ParamType.SQL
        assert classify("SELECT") != ParamType.SQL   # too short, single word

    # --- INTEGER ---
    def test_integer(self):         assert classify("100")     == ParamType.INTEGER
    def test_negative_int(self):    assert classify("-42")     == ParamType.INTEGER
    def test_zero(self):            assert classify("0")       in (ParamType.INTEGER, ParamType.BOOLEAN)

    # --- DECIMAL ---
    def test_decimal(self):         assert classify("3.14")    == ParamType.DECIMAL
    def test_small_decimal(self):   assert classify("0.005")   == ParamType.DECIMAL
    def test_negative_decimal(self):assert classify("-99.99")  == ParamType.DECIMAL

    # --- BOOLEAN ---
    def test_bool_y(self):          assert classify("Y")       == ParamType.BOOLEAN
    def test_bool_no(self):         assert classify("NO")      == ParamType.BOOLEAN
    def test_bool_true(self):       assert classify("TRUE")    == ParamType.BOOLEAN
    def test_bool_false(self):      assert classify("FALSE")   == ParamType.BOOLEAN

    # --- PATH ---
    def test_unix_path(self):       assert classify("/data/input/sales/") == ParamType.PATH
    def test_windows_path(self):    assert classify("C:\\data\\input\\") == ParamType.PATH
    def test_s3_path(self):         assert classify("s3://my-bucket/data/") == ParamType.PATH
    def test_pm_variable(self):     assert classify("$PMRootDir/logs") == ParamType.PATH

    # --- EMPTY ---
    def test_empty_string(self):    assert classify("")   == ParamType.EMPTY
    def test_whitespace(self):      assert classify("  ") == ParamType.EMPTY

    # --- STRING ---
    def test_plain_string(self):    assert classify("FULL")        == ParamType.STRING
    def test_env_string(self):      assert classify("DEV")         == ParamType.STRING
    def test_region_list(self):     assert classify("NORTH,SOUTH") == ParamType.STRING

    # --- Fixture params ---
    def test_fixture_dates_classified(self, workflow_section):
        assert workflow_section.params["START_DATE"].param_type == ParamType.DATE
        assert workflow_section.params["RUN_DATE"].param_type   == ParamType.DATE
        assert workflow_section.params["ORACLE_RUN_DATE"].param_type == ParamType.DATE

    def test_fixture_masks_classified(self, workflow_section):
        assert workflow_section.params["DATE_MASK"].param_type     == ParamType.DATE_MASK
        assert workflow_section.params["DATETIME_MASK"].param_type == ParamType.DATE_MASK

    def test_fixture_sql_classified(self, load_session):
        assert load_session.params["FILTER_CONDITION"].param_type == ParamType.SQL
        assert load_session.params["LOOKUP_SQL"].param_type       == ParamType.SQL
        assert load_session.params["OVERRIDE_SQL"].param_type     == ParamType.SQL

    def test_fixture_multiline_sql_classified(self, enrich_session):
        assert enrich_session.params["ENRICH_SQL"].param_type == ParamType.SQL

    def test_fixture_integer(self, load_session):
        assert load_session.params["INT_BATCH_SIZE"].param_type == ParamType.INTEGER

    def test_fixture_decimal(self, load_session):
        assert load_session.params["DECIMAL_THRESHOLD"].param_type == ParamType.DECIMAL

    def test_fixture_path(self, workflow_section):
        assert workflow_section.params["LOG_DIR"].param_type    == ParamType.PATH
        assert workflow_section.params["SOURCE_DIR"].param_type == ParamType.PATH


# ---------------------------------------------------------------------------
# Normalizer tests
# ---------------------------------------------------------------------------

class TestNormalizer:
    def test_us_date_normalized_to_iso(self, workflow_section):
        p = workflow_section.params["START_DATE"]
        assert p.normalized_value == "2024-01-01"

    def test_oracle_date_normalized(self, workflow_section):
        p = workflow_section.params["ORACLE_RUN_DATE"]
        assert p.normalized_value == "2024-03-23"

    def test_compact_date_normalized(self, workflow_section):
        p = workflow_section.params["COMPACT_DATE"]
        assert p.normalized_value == "2024-03-23"

    def test_iso_date_unchanged(self, workflow_section):
        p = workflow_section.params["RUN_DATE"]
        assert p.normalized_value == "2024-03-23"

    def test_date_mask_translated(self, workflow_section):
        p = workflow_section.params["DATE_MASK"]
        # MM/DD/YYYY → MM/dd/yyyy (Java SimpleDateFormat)
        assert "yyyy" in p.spark_value
        assert "dd" in p.spark_value

    def test_oracle_datetime_mask_translated(self, workflow_section):
        p = workflow_section.params["ORACLE_MASK"]
        assert "yyyy" in p.spark_value
        assert "HH" in p.spark_value
        assert "mm" in p.spark_value or "ss" in p.spark_value

    def test_boolean_y_normalized(self, global_section):
        p = global_section.params["BATCH_ENABLED"]
        assert p.normalized_value == "true"

    def test_boolean_n_normalized(self, patient_workflow):
        # PHI_MASKING=Y should normalize to true
        p = patient_workflow.params["PHI_MASKING"]
        assert p.normalized_value == "true"

    def test_integer_unchanged(self, global_section):
        p = global_section.params["MAX_ERRORS"]
        assert p.normalized_value == "100"

    def test_decimal_unchanged(self, load_session):
        p = load_session.params["DECIMAL_THRESHOLD"]
        assert p.normalized_value == "0.005"

    def test_sql_multiline_collapsed(self, enrich_session):
        p = enrich_session.params["ENRICH_SQL"]
        assert "\n" not in p.normalized_value
        assert "INNER JOIN" in p.normalized_value

    def test_sql_has_spark_value(self, load_session):
        p = load_session.params["FILTER_CONDITION"]
        assert p.spark_value  # not empty
        assert len(p.spark_value) > 5

    def test_pm_path_translated(self, workflow_section):
        p = workflow_section.params["LOG_DIR"]
        assert "s3://" in p.spark_value or "infa" in p.spark_value

    def test_s3_path_unchanged(self, patient_workflow):
        p = patient_workflow.params["OUTPUT_PATH"]
        assert p.normalized_value == "s3://analytics-lake/patient/extract/"

    def test_glue_arg_name_set(self, workflow_section):
        p = workflow_section.params["START_DATE"]
        assert p.glue_arg_name == "START_DATE"

    def test_glue_arg_name_no_dollar(self, prm):
        for section in prm.sections:
            for param in section.params.values():
                assert "$" not in param.glue_arg_name

    def test_date_notes_populated(self, workflow_section):
        p = workflow_section.params["START_DATE"]
        assert len(p.notes) > 0

    def test_mask_notes_populated(self, workflow_section):
        p = workflow_section.params["DATE_MASK"]
        assert any("SimpleDateFormat" in n or "mask" in n.lower() for n in p.notes)

    def test_path_notes_populated(self, workflow_section):
        p = workflow_section.params["LOG_DIR"]
        assert any("s3" in n.lower() or "path" in n.lower() or "PM" in n for n in p.notes)

    # Direct normalizer unit tests
    def test_normalize_date_us(self):
        iso, spark, notes = _normalize_date("03/23/2024")
        assert iso == "2024-03-23"
        assert "MM/DD/YYYY" in notes[0]

    def test_normalize_date_oracle_two_digit_year(self):
        iso, spark, notes = _normalize_date("15-JAN-24")
        assert iso.startswith("20")
        assert any("expanded" in n.lower() for n in notes)

    def test_normalize_date_mask_hh24(self):
        _, spark, _ = _normalize_date_mask("MM/DD/YYYY HH24:MI:SS")
        assert "HH" in spark    # HH24 → HH
        assert "mm" in spark    # MI → mm
        assert "ss" in spark    # SS → ss
        assert "yyyy" in spark  # YYYY → yyyy


# ---------------------------------------------------------------------------
# Exporter tests
# ---------------------------------------------------------------------------

class TestExporter:
    def test_full_params_json_written(self, prm, tmp_path):
        out = write_full_params_json(prm, tmp_path)
        assert out.exists()
        data = json.loads(out.read_text())
        assert "_metadata" in data
        assert "sections" in data
        assert "merged" in data

    def test_full_params_has_all_sections(self, prm, tmp_path):
        out = write_full_params_json(prm, tmp_path)
        data = json.loads(out.read_text())
        assert len(data["sections"]) == len(prm.sections)

    def test_glue_params_json_written(self, prm, tmp_path):
        paths = write_glue_params_json(prm, tmp_path)
        assert len(paths) > 0
        for p in paths:
            assert p.exists()

    def test_glue_params_has_metadata(self, prm, tmp_path):
        paths = write_glue_params_json(prm, tmp_path)
        for p in paths:
            data = json.loads(p.read_text())
            assert "_metadata" in data

    def test_glue_params_flat_structure(self, prm, tmp_path):
        paths = write_glue_params_json(prm, tmp_path)
        merged_path = next(p for p in paths if "_merged" in str(p))
        data = json.loads(merged_path.read_text())
        # Every non-metadata key should have value/type/spark_value
        for k, v in data.items():
            if k == "_metadata":
                continue
            assert "value" in v
            assert "type" in v
            assert "spark_value" in v

    def test_export_all_produces_all_artifacts(self, prm, tmp_path):
        written = export_all([prm], tmp_path)
        assert written["full_params"]
        assert written["glue_params"]
        assert written["loader"]
        assert written["tf_snippets"]
        assert written["report"]

    def test_param_loader_py_written(self, prm, tmp_path):
        export_all([prm], tmp_path)
        loader = tmp_path / "param_loader.py"
        assert loader.exists()
        content = loader.read_text()
        assert "def load_params" in content
        assert "def merge_with_args" in content
        assert "def get_param" in content
        assert "boto3" in content

    def test_terraform_snippet_written(self, prm, tmp_path):
        export_all([prm], tmp_path)
        snippets = list((tmp_path / "terraform-snippets").rglob("*.tf.txt"))
        assert len(snippets) > 0
        content = snippets[0].read_text()
        assert "PARAMS_S3_PATH" in content

    def test_translation_report_written(self, prm, tmp_path):
        export_all([prm], tmp_path)
        report = tmp_path / "translation-report.json"
        assert report.exists()
        data = json.loads(report.read_text())
        assert data["summary"]["total_params"] > 0
        assert "type_distribution" in data["summary"]

    def test_report_has_sql_type(self, prm, tmp_path):
        export_all([prm], tmp_path)
        report = tmp_path / "translation-report.json"
        data = json.loads(report.read_text())
        dist = data["summary"]["type_distribution"]
        assert "SQL" in dist
        assert dist["SQL"] >= 3   # fixture has several SQL params

    def test_glue_params_folder_structure(self, prm, tmp_path):
        export_all([prm], tmp_path)
        sales_dir = tmp_path / "glue-params" / "SALES_MART"
        assert sales_dir.exists()
        assert any(f.suffix == ".json" for f in sales_dir.iterdir())

    def test_merged_json_respects_precedence(self, prm, tmp_path):
        export_all([prm], tmp_path)
        merged_file = tmp_path / "glue-params" / "_merged" / "sample.json"
        assert merged_file.exists()
        data = json.loads(merged_file.read_text())
        # START_DATE: session (01/15/2024 → 2024-01-15) should win over workflow (01/01/2024)
        assert data["START_DATE"]["value"] == "2024-01-15"

    def test_multiple_files_export(self, tmp_path):
        text_a = "[Global]\n$$FOO=bar\n"
        text_b = "[FOLDER.WF]\n$$BAZ=2024-01-01\n"
        from param_translator.parser import parse_prm_text
        prm_a = parse_prm_text(text_a, "a.prm")
        prm_b = parse_prm_text(text_b, "b.prm")
        classify_file(prm_a); normalize_file(prm_a)
        classify_file(prm_b); normalize_file(prm_b)
        written = export_all([prm_a, prm_b], tmp_path)
        assert len(written["full_params"]) == 2
