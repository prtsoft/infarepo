"""
Tests for validation-harness — recon, schema diff, rules, runner, reporter.
Run with:  pytest tests/test_validation_harness.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from textwrap import dedent
from typing import List, Sequence

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

from validation_harness.connections import MockConnection, MockResult, create_connection
from validation_harness.models import (
    ReconResult, SchemaField, SchemaDiff, TypeMismatch, RuleResult,
    ValidationReport, ValidationSummary, _redact_dsn,
)
from validation_harness.recon import (
    count_rows, run_recon, diff_schemas, normalize_type, types_compatible, get_schema,
)
from validation_harness.rules import (
    NullCheckConfig, RangeCheckConfig, UniqueCheckConfig,
    ValueSetConfig, ReferentialConfig, CustomSQLConfig,
    evaluate_null_check, evaluate_range_check, evaluate_unique_check,
    evaluate_value_set, evaluate_referential, evaluate_custom_sql,
    build_and_evaluate,
)
from validation_harness.runner import run_validation, load_config, _expand_env
from validation_harness.reporter import write_json_report, write_text_summary

FIXTURE_DIR = Path(__file__).parent / "fixtures"
RULES_YAML  = FIXTURE_DIR / "validation_rules.yaml"


# ===========================================================================
# Helpers
# ===========================================================================

def _conn(count_map: dict) -> MockConnection:
    """Create a MockConnection with COUNT(*) results pre-registered."""
    c = MockConnection()
    for fragment, count in count_map.items():
        c.set_result(fragment, [(count,)])
    return c


def _schema_rows(cols: List[tuple]) -> List[tuple]:
    """Build INFORMATION_SCHEMA-style rows: (name, type, nullable)."""
    return list(cols)


# ===========================================================================
# TestModels
# ===========================================================================

class TestModels:
    def test_redact_dsn_sqlalchemy(self):
        dsn = "mssql+pyodbc://etl_user:s3cr3t@host:1433/DB"
        redacted = _redact_dsn(dsn)
        assert "s3cr3t" not in redacted
        assert "etl_user" in redacted
        assert "***" in redacted

    def test_redact_dsn_token(self):
        dsn = "databricks://token:dapi_secret@workspace.net/db"
        redacted = _redact_dsn(dsn)
        assert "dapi_secret" not in redacted
        assert "***" in redacted

    def test_redact_dsn_odbc_pwd(self):
        dsn = "mssql://server;PWD=mypassword;UID=user"
        redacted = _redact_dsn(dsn)
        assert "mypassword" not in redacted

    def test_redact_dsn_no_password(self):
        dsn = "sqlite:///test.db"
        assert _redact_dsn(dsn) == dsn

    def test_rule_result_make_pass(self):
        r = RuleResult.make("test", "null_check", "orders", "ORDER_ID",
                             failing_count=0, total_count=1000)
        assert r.passed is True
        assert r.fail_pct == 0.0
        assert r.hipaa_flagged is False

    def test_rule_result_make_fail(self):
        r = RuleResult.make("test", "null_check", "orders", "ORDER_ID",
                             failing_count=10, total_count=1000)
        assert r.passed is False
        assert r.fail_pct == pytest.approx(1.0)

    def test_rule_result_hipaa_flagged(self):
        r = RuleResult.make("ssn check", "null_check", "patient", "SSN",
                             failing_count=0, total_count=500)
        assert r.hipaa_flagged is True
        assert "HIPAA" in r.message

    def test_rule_result_phi_column(self):
        r = RuleResult.make("phi check", "null_check", "patient", "PHI_DATA",
                             failing_count=0, total_count=100)
        assert r.hipaa_flagged is True

    def test_rule_result_pii_column(self):
        r = RuleResult.make("name check", "null_check", "patient", "PATIENT_NAME",
                             failing_count=0, total_count=100)
        assert r.hipaa_flagged is True

    def test_rule_result_normal_column_not_flagged(self):
        r = RuleResult.make("amt check", "range_check", "orders", "ORDER_AMT",
                             failing_count=0, total_count=100)
        assert r.hipaa_flagged is False

    def test_validation_summary_to_dict(self):
        s = ValidationSummary(
            files_validated=1,
            recon_total=2, recon_passed=2,
            schema_diff_total=2, schema_diff_passed=1,
            rules_total=5, rules_passed=4,
            hipaa_flags=1, overall_passed=False,
        )
        d = s.to_dict()
        assert d["recon_failed"] == 0
        assert d["schema_diff_failed"] == 1
        assert d["rules_failed"] == 1
        assert d["overall_passed"] is False

    def test_report_to_dict_structure(self):
        s = ValidationSummary(1,1,1,0,0,0,0,0,True)
        r = ValidationReport(
            generated="2024-01-01T00:00:00",
            source_dsn="mssql://***",
            target_dsn="databricks://***",
            config_path="test.yaml",
            recon_results=[],
            schema_diffs=[],
            rule_results=[],
            summary=s,
        )
        d = r.to_dict()
        assert "_generated" in d
        assert "summary" in d
        assert "recon" in d
        assert "schema_diffs" in d
        assert "rules" in d


# ===========================================================================
# TestConnections
# ===========================================================================

class TestConnections:
    def test_mock_connection_scalar(self):
        conn = MockConnection()
        conn.set_result("count(*) from orders", [(42,)])
        result = conn.execute("SELECT count(*) FROM orders")
        assert result.scalar() == 42

    def test_mock_connection_default(self):
        conn = MockConnection()
        conn.set_default([(99,)])
        result = conn.execute("SELECT * FROM anything")
        assert result.scalar() == 99

    def test_mock_connection_fetchall(self):
        conn = MockConnection()
        conn.set_result("information_schema", [("ORDER_ID", "int", "NO"), ("AMT", "decimal", "YES")])
        result = conn.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='orders'")
        rows = result.fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "ORDER_ID"

    def test_mock_connection_audit_trail(self):
        conn = MockConnection()
        conn.execute("SELECT COUNT(*) FROM orders")
        conn.execute("SELECT COUNT(*) FROM customers")
        assert len(conn.executed) == 2

    def test_create_connection_mock(self):
        conn = create_connection("mock://")
        assert isinstance(conn, MockConnection)

    def test_create_connection_mock_flag(self):
        conn = create_connection("mssql+pyodbc://user:pass@host/db", mock=True)
        assert isinstance(conn, MockConnection)


# ===========================================================================
# TestRecon
# ===========================================================================

class TestRecon:
    def test_count_rows_basic(self):
        conn = _conn({"count(*) from orders": 500})
        assert count_rows(conn, "orders") == 500

    def test_count_rows_with_where(self):
        conn = MockConnection()
        conn.set_default([(250,)])
        assert count_rows(conn, "orders", "STATUS = 'ACTIVE'") == 250
        assert "WHERE" in conn.executed[-1]

    def test_run_recon_pass(self):
        src = _conn({"count(*) from dbo.orders": 1000})
        tgt = _conn({"count(*) from orders": 1000})
        result = run_recon(src, tgt, "dbo.ORDERS", "orders", tolerance_pct=1.0)
        assert result.passed is True
        assert result.source_count == 1000
        assert result.target_count == 1000
        assert result.delta == 0

    def test_run_recon_within_tolerance(self):
        src = _conn({"count(*) from dbo.orders": 1000})
        tgt = _conn({"count(*) from orders": 995})
        result = run_recon(src, tgt, "dbo.ORDERS", "orders", tolerance_pct=1.0)
        assert result.passed is True
        assert result.delta == -5
        assert result.delta_pct == pytest.approx(0.5)

    def test_run_recon_fail_exceeds_tolerance(self):
        src = _conn({"count(*) from dbo.orders": 1000})
        tgt = _conn({"count(*) from orders": 900})
        result = run_recon(src, tgt, "dbo.ORDERS", "orders", tolerance_pct=1.0)
        assert result.passed is False
        assert result.delta == -100
        assert result.delta_pct == pytest.approx(10.0)
        assert any("exceeds tolerance" in n for n in result.notes)

    def test_run_recon_table_pair_format(self):
        src = _conn({"count(*) from dbo.orders": 1000})
        tgt = _conn({"count(*) from orders": 1000})
        result = run_recon(src, tgt, "dbo.ORDERS", "orders")
        assert "→" in result.table_pair

    def test_run_recon_zero_source_warns(self):
        src = _conn({"count(*) from dbo.orders": 0})
        tgt = _conn({"count(*) from orders": 0})
        result = run_recon(src, tgt, "dbo.ORDERS", "orders")
        assert result.passed is True
        assert any("source count is 0" in n for n in result.notes)

    def test_run_recon_custom_sql(self):
        src = MockConnection()
        src.set_result("select count(*) from dbo.orders where active=1", [(500,)])
        tgt = _conn({"count(*) from orders": 500})
        result = run_recon(
            src, tgt, "dbo.ORDERS", "orders",
            source_sql="SELECT COUNT(*) FROM dbo.ORDERS WHERE ACTIVE=1",
        )
        assert result.source_count == 500

    def test_normalize_type_varchar(self):
        assert normalize_type("varchar") == "string"
        assert normalize_type("NVARCHAR(255)") == "string"

    def test_normalize_type_int(self):
        assert normalize_type("int") == "integer"
        assert normalize_type("BIGINT") == "integer"

    def test_normalize_type_decimal(self):
        assert normalize_type("decimal(18,2)") == "decimal"
        assert normalize_type("FLOAT") == "decimal"

    def test_normalize_type_datetime(self):
        assert normalize_type("datetime2") == "timestamp"
        assert normalize_type("DATE") == "date"

    def test_normalize_type_bit(self):
        assert normalize_type("bit") == "boolean"

    def test_types_compatible_same(self):
        assert types_compatible("string", "string") is True

    def test_types_compatible_string_variants(self):
        assert types_compatible("nvarchar", "string") is True

    def test_types_compatible_bool_int(self):
        assert types_compatible("boolean", "integer") is True

    def test_types_incompatible(self):
        assert types_compatible("string", "integer") is False

    def test_diff_schemas_pass(self):
        src = MockConnection()
        tgt = MockConnection()
        src.set_result("information_schema", [
            ("ORDER_ID", "int", "NO"),
            ("AMT",      "decimal", "YES"),
        ])
        tgt.set_result("information_schema", [
            ("ORDER_ID", "int", "NO"),
            ("AMT",      "decimal", "YES"),
        ])
        diff = diff_schemas(src, tgt, "dbo.ORDERS", "orders")
        assert diff.passed is True
        assert diff.missing_in_target == []
        assert diff.type_mismatches == []

    def test_diff_schemas_missing_column(self):
        src = MockConnection()
        tgt = MockConnection()
        src.set_result("information_schema", [
            ("ORDER_ID", "int", "NO"),
            ("REGION",   "varchar", "YES"),
        ])
        tgt.set_result("information_schema", [
            ("ORDER_ID", "int", "NO"),
            # REGION missing in target
        ])
        diff = diff_schemas(src, tgt, "dbo.ORDERS", "orders")
        assert diff.passed is False
        assert "REGION" in diff.missing_in_target

    def test_diff_schemas_extra_column_ok(self):
        src = MockConnection()
        tgt = MockConnection()
        src.set_result("information_schema", [("ORDER_ID", "int", "NO")])
        tgt.set_result("information_schema", [
            ("ORDER_ID",    "int",      "NO"),
            ("ETL_INSERT_DT", "datetime", "YES"),  # extra — should be OK when ignored
        ])
        diff = diff_schemas(src, tgt, "dbo.ORDERS", "orders",
                            ignored_columns=["ETL_INSERT_DT"])
        assert diff.passed is True
        assert "ETL_INSERT_DT" in diff.ignored_columns

    def test_diff_schemas_compatible_type_mismatch(self):
        # datetime2 (→ timestamp) vs date — different normalized types but in _COMPATIBLE_PAIRS
        src = MockConnection()
        tgt = MockConnection()
        src.set_result("information_schema", [("RUN_DT", "datetime2", "YES")])
        tgt.set_result("information_schema", [("RUN_DT", "date",      "YES")])
        diff = diff_schemas(src, tgt, "dbo.ORDERS", "orders")
        assert diff.passed is True
        assert len(diff.type_mismatches) == 1
        assert diff.type_mismatches[0].compatible is True

    def test_diff_schemas_incompatible_type_mismatch(self):
        src = MockConnection()
        tgt = MockConnection()
        src.set_result("information_schema", [("AMOUNT", "varchar", "YES")])
        tgt.set_result("information_schema", [("AMOUNT", "integer", "YES")])
        diff = diff_schemas(src, tgt, "dbo.ORDERS", "orders")
        assert diff.passed is False
        assert not diff.type_mismatches[0].compatible


# ===========================================================================
# TestRules
# ===========================================================================

class TestRules:
    def test_null_check_pass(self):
        conn = _conn({"is null": 0, "count(*) from orders": 1000})
        cfg = NullCheckConfig(name="no null IDs", column="ORDER_ID")
        result = evaluate_null_check(conn, "orders", cfg)
        assert result.passed is True
        assert result.failing_count == 0

    def test_null_check_fail(self):
        conn = MockConnection()
        conn.set_result("is null", [(50,)])
        conn.set_default([(1000,)])
        cfg = NullCheckConfig(name="no null IDs", column="ORDER_ID")
        result = evaluate_null_check(conn, "orders", cfg)
        assert result.passed is False
        assert result.failing_count == 50

    def test_null_check_hipaa_column(self):
        conn = _conn({"is null": 0, "count(*) from patient": 500})
        cfg = NullCheckConfig(name="no null SSN", column="SSN")
        result = evaluate_null_check(conn, "patient", cfg)
        assert result.hipaa_flagged is True

    def test_range_check_pass(self):
        conn = _conn({"< 0": 0, "count(*) from orders": 1000})
        cfg = RangeCheckConfig(name="positive amounts", column="ORDER_AMT", min=0)
        result = evaluate_range_check(conn, "orders", cfg)
        assert result.passed is True

    def test_range_check_fail(self):
        conn = MockConnection()
        conn.set_result("< 0", [(25,)])
        conn.set_default([(1000,)])
        cfg = RangeCheckConfig(name="positive amounts", column="ORDER_AMT", min=0)
        result = evaluate_range_check(conn, "orders", cfg)
        assert result.passed is False
        assert result.failing_count == 25

    def test_range_check_no_bounds_trivially_passes(self):
        conn = _conn({"count(*) from orders": 100})
        cfg = RangeCheckConfig(name="no bounds", column="AMT")
        result = evaluate_range_check(conn, "orders", cfg)
        assert result.passed is True

    def test_unique_check_pass(self):
        conn = _conn({"count(*) - count(distinct": 0, "count(*) from orders": 1000})
        cfg = UniqueCheckConfig(name="unique orders", column="ORDER_ID")
        result = evaluate_unique_check(conn, "orders", cfg)
        assert result.passed is True

    def test_unique_check_fail(self):
        conn = MockConnection()
        conn.set_result("count(*) - count(distinct", [(10,)])
        conn.set_default([(1000,)])
        cfg = UniqueCheckConfig(name="unique orders", column="ORDER_ID")
        result = evaluate_unique_check(conn, "orders", cfg)
        assert result.passed is False
        assert result.failing_count == 10

    def test_value_set_pass(self):
        conn = _conn({"not in": 0, "count(*) from orders": 500})
        cfg = ValueSetConfig(name="valid status", column="STATUS",
                             allowed_values=["PENDING", "APPROVED"])
        result = evaluate_value_set(conn, "orders", cfg)
        assert result.passed is True

    def test_value_set_fail(self):
        conn = MockConnection()
        conn.set_result("not in", [(7,)])
        conn.set_default([(500,)])
        cfg = ValueSetConfig(name="valid status", column="STATUS",
                             allowed_values=["PENDING", "APPROVED"])
        result = evaluate_value_set(conn, "orders", cfg)
        assert result.passed is False
        assert result.failing_count == 7

    def test_value_set_empty_values_passes(self):
        conn = _conn({"count(*) from orders": 100})
        cfg = ValueSetConfig(name="empty set", column="STATUS", allowed_values=[])
        result = evaluate_value_set(conn, "orders", cfg)
        assert result.passed is True

    def test_referential_pass(self):
        conn = _conn({"not in": 0, "count(*) from orders": 1000})
        cfg = ReferentialConfig(
            name="customer FK", column="CUSTOMER_ID",
            parent_table="customers", parent_column="CUSTOMER_ID",
        )
        result = evaluate_referential(conn, "orders", cfg)
        assert result.passed is True

    def test_referential_fail(self):
        conn = MockConnection()
        conn.set_result("not in", [(3,)])
        conn.set_default([(1000,)])
        cfg = ReferentialConfig(
            name="customer FK", column="CUSTOMER_ID",
            parent_table="customers", parent_column="CUSTOMER_ID",
        )
        result = evaluate_referential(conn, "orders", cfg)
        assert result.passed is False
        assert result.failing_count == 3

    def test_custom_sql_pass(self):
        conn = MockConnection()
        conn.set_result("current_date", [(0,)])
        conn.set_default([(1000,)])
        cfg = CustomSQLConfig(
            name="no future dates",
            sql="SELECT COUNT(*) FROM {table} WHERE ORDER_DATE > CURRENT_DATE",
            expect_count=0,
        )
        result = evaluate_custom_sql(conn, "orders", cfg)
        assert result.passed is True

    def test_custom_sql_fail(self):
        conn = MockConnection()
        conn.set_result("current_date", [(5,)])
        conn.set_default([(1000,)])
        cfg = CustomSQLConfig(
            name="no future dates",
            sql="SELECT COUNT(*) FROM {table} WHERE ORDER_DATE > CURRENT_DATE",
            expect_count=0,
        )
        result = evaluate_custom_sql(conn, "orders", cfg)
        assert result.passed is False
        assert result.failing_count == 5

    def test_custom_sql_table_placeholder(self):
        conn = MockConnection()
        conn.set_default([(0,)])
        cfg = CustomSQLConfig(
            name="custom check",
            sql="SELECT COUNT(*) FROM {table} WHERE X = 1",
            expect_count=0,
        )
        evaluate_custom_sql(conn, "my_table", cfg)
        assert "my_table" in conn.executed[-2]   # count(*) from my_table

    def test_build_and_evaluate_null_check(self):
        conn = _conn({"is null": 0, "count(*) from orders": 100})
        result = build_and_evaluate(conn, "orders", {
            "type": "null_check", "name": "test", "column": "ID",
        })
        assert result.rule_type == "null_check"

    def test_build_and_evaluate_range_check(self):
        conn = _conn({"< 0": 0, "count(*) from orders": 100})
        result = build_and_evaluate(conn, "orders", {
            "type": "range_check", "name": "test", "column": "AMT", "min": 0,
        })
        assert result.rule_type == "range_check"

    def test_build_and_evaluate_unknown_type(self):
        conn = MockConnection()
        with pytest.raises(ValueError, match="Unknown rule type"):
            build_and_evaluate(conn, "orders", {"type": "nonexistent", "name": "x"})

    def test_unsafe_table_identifier_rejected(self):
        conn = MockConnection()
        cfg = NullCheckConfig(name="test", column="ORDER_ID")
        with pytest.raises(ValueError, match="Unsafe table identifier"):
            evaluate_null_check(conn, "orders; DROP TABLE orders--", cfg)

    def test_unsafe_column_identifier_rejected(self):
        conn = MockConnection()
        cfg = NullCheckConfig(name="test", column="ID; DROP TABLE x--")
        with pytest.raises(ValueError, match="Unsafe column identifier"):
            evaluate_null_check(conn, "orders", cfg)


# ===========================================================================
# TestRunner
# ===========================================================================

class TestRunner:
    def _write_config(self, tmp_path: Path, content: str) -> Path:
        cfg = tmp_path / "test_rules.yaml"
        cfg.write_text(content, encoding="utf-8")
        return cfg

    def test_load_config_basic(self, tmp_path):
        cfg = self._write_config(tmp_path, dedent("""
            version: "1"
            connections:
              source:
                dsn: "mssql://user:pass@host/db"
            validations: []
        """))
        data = load_config(cfg)
        assert data["version"] == "1"
        assert "connections" in data

    def test_expand_env(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "abc123")
        result = _expand_env("token:${MY_SECRET}@host")
        assert "abc123" in result

    def test_expand_env_missing_var(self):
        result = _expand_env("${DEFINITELY_NOT_SET_VAR_XYZ}")
        assert "${DEFINITELY_NOT_SET_VAR_XYZ}" in result   # left unchanged

    def test_run_validation_recon_only(self, tmp_path):
        cfg = self._write_config(tmp_path, dedent("""
            version: "1"
            connections:
              source:
                dsn: "mock://"
              target:
                dsn: "mock://"
            validations:
              - source_table: "dbo.ORDERS"
                target_table: "orders"
                recon:
                  tolerance_pct: 1.0
        """))
        src = _conn({"count(*) from dbo.orders": 1000})
        tgt = _conn({"count(*) from orders": 1000})
        report = run_validation(cfg, source_conn=src, target_conn=tgt, run_recon_only=True)
        assert len(report.recon_results) == 1
        assert report.recon_results[0].passed is True
        assert report.schema_diffs == []
        assert report.rule_results == []

    def test_run_validation_full(self, tmp_path):
        cfg = self._write_config(tmp_path, dedent("""
            version: "1"
            connections:
              source:
                dsn: "mock://"
              target:
                dsn: "mock://"
            validations:
              - source_table: "dbo.ORDERS"
                target_table: "orders"
                recon:
                  tolerance_pct: 1.0
                schema_diff:
                  enabled: true
                rules:
                  - name: "no nulls"
                    type: null_check
                    column: ORDER_ID
        """))
        src = MockConnection()
        src.set_result("information_schema", [("ORDER_ID", "int", "NO")])
        src.set_result("count(*) from dbo.orders", [(500,)])

        tgt = MockConnection()
        # Register specific fragments first so they win over the broad count key
        tgt.set_result("information_schema", [("ORDER_ID", "int", "NO")])
        tgt.set_result("is null", [(0,)])
        tgt.set_result("count(*) from orders", [(500,)])
        tgt.set_default([(500,)])

        report = run_validation(cfg, source_conn=src, target_conn=tgt)
        assert report.summary.recon_total == 1
        assert report.summary.schema_diff_total == 1
        assert report.summary.rules_total == 1
        assert report.summary.overall_passed is True

    def test_run_validation_summary_overall_pass(self, tmp_path):
        cfg = self._write_config(tmp_path, dedent("""
            version: "1"
            connections:
              source:
                dsn: "mock://"
              target:
                dsn: "mock://"
            validations:
              - source_table: "src"
                target_table: "tgt"
                recon:
                  tolerance_pct: 1.0
        """))
        src = _conn({"count(*) from src": 100})
        tgt = _conn({"count(*) from tgt": 100})
        report = run_validation(cfg, source_conn=src, target_conn=tgt, run_recon_only=True)
        assert report.summary.overall_passed is True

    def test_run_validation_summary_overall_fail(self, tmp_path):
        cfg = self._write_config(tmp_path, dedent("""
            version: "1"
            connections:
              source:
                dsn: "mock://"
              target:
                dsn: "mock://"
            validations:
              - source_table: "src"
                target_table: "tgt"
                recon:
                  tolerance_pct: 0.0
        """))
        src = _conn({"count(*) from src": 1000})
        tgt = _conn({"count(*) from tgt": 900})
        report = run_validation(cfg, source_conn=src, target_conn=tgt, run_recon_only=True)
        assert report.summary.overall_passed is False

    def test_dry_run_uses_mock_connection(self, tmp_path):
        """dry_run=True should not attempt a real DB connection."""
        cfg = self._write_config(tmp_path, dedent("""
            version: "1"
            connections:
              source:
                dsn: "mssql+pyodbc://user:pass@nonexistent_host/db"
              target:
                dsn: "databricks://token:abc@nonexistent.net/db"
            validations: []
        """))
        # Should not raise even though DSNs are fake
        report = run_validation(cfg, dry_run=True)
        assert report is not None

    def test_hipaa_flags_counted(self, tmp_path):
        cfg = self._write_config(tmp_path, dedent("""
            version: "1"
            connections:
              source:
                dsn: "mock://"
              target:
                dsn: "mock://"
            validations:
              - source_table: "dbo.PATIENT"
                target_table: "patient"
                recon:
                  tolerance_pct: 0.0
                schema_diff:
                  enabled: false
                rules:
                  - name: "no null SSN"
                    type: null_check
                    column: SSN
        """))
        src = _conn({"count(*) from dbo.patient": 100})
        tgt = MockConnection()
        # Register specific fragment before broad count so null check matches correctly
        tgt.set_result("is null", [(0,)])
        tgt.set_result("count(*) from patient", [(100,)])
        tgt.set_default([(100,)])

        report = run_validation(cfg, source_conn=src, target_conn=tgt, run_recon_only=False)
        assert report.summary.hipaa_flags >= 1

    def test_credentials_redacted_in_report(self, tmp_path):
        cfg = self._write_config(tmp_path, dedent("""
            version: "1"
            connections:
              source:
                dsn: "mssql+pyodbc://user:SUPERSECRET@host/db"
              target:
                dsn: "mock://"
            validations: []
        """))
        report = run_validation(cfg, dry_run=True)
        assert "SUPERSECRET" not in report.source_dsn


# ===========================================================================
# TestReporter
# ===========================================================================

class TestReporter:
    def _make_report(self) -> ValidationReport:
        s = ValidationSummary(
            files_validated=1,
            recon_total=1, recon_passed=1,
            schema_diff_total=1, schema_diff_passed=1,
            rules_total=2, rules_passed=2,
            hipaa_flags=1, overall_passed=True,
        )
        recon = ReconResult(
            table_pair="dbo.ORDERS → orders",
            source_table="dbo.ORDERS",
            target_table="orders",
            source_count=1000,
            target_count=1000,
            delta=0,
            delta_pct=0.0,
            tolerance_pct=1.0,
            passed=True,
            notes=[],
        )
        rule = RuleResult(
            name="no null IDs", rule_type="null_check",
            table="orders", column="ORDER_ID",
            passed=True, failing_count=0, total_count=1000,
            fail_pct=0.0, message="0/1000 rows failing",
            hipaa_flagged=False,
        )
        diff = SchemaDiff(
            table_pair="dbo.ORDERS → orders",
            source_table="dbo.ORDERS",
            target_table="orders",
            source_columns=[], target_columns=[],
            missing_in_target=[], extra_in_target=[],
            type_mismatches=[], ignored_columns=[],
            passed=True,
        )
        return ValidationReport(
            generated="2024-03-23T00:00:00Z",
            source_dsn="mssql://***",
            target_dsn="databricks://***",
            config_path="test.yaml",
            recon_results=[recon],
            schema_diffs=[diff],
            rule_results=[rule],
            summary=s,
        )

    def test_json_report_written(self, tmp_path):
        report = self._make_report()
        path = write_json_report(report, tmp_path)
        assert path.exists()
        data = json.loads(path.read_text())
        assert "_generated" in data
        assert data["summary"]["overall_passed"] is True

    def test_json_report_no_credentials(self, tmp_path):
        report = self._make_report()
        path = write_json_report(report, tmp_path)
        content = path.read_text()
        assert "SUPERSECRET" not in content

    def test_json_report_recon_structure(self, tmp_path):
        report = self._make_report()
        path = write_json_report(report, tmp_path)
        data = json.loads(path.read_text())
        assert len(data["recon"]) == 1
        r = data["recon"][0]
        assert r["source_count"] == 1000
        assert r["passed"] is True

    def test_json_report_rules_structure(self, tmp_path):
        report = self._make_report()
        path = write_json_report(report, tmp_path)
        data = json.loads(path.read_text())
        assert len(data["rules"]) == 1
        rule = data["rules"][0]
        # Values never appear — only counts
        assert "failing_count" in rule
        assert "fail_pct" in rule

    def test_text_summary_written(self, tmp_path):
        report = self._make_report()
        path = write_text_summary(report, tmp_path)
        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert "PASS" in content
        assert "dbo.ORDERS" in content
        assert "orders" in content

    def test_text_summary_shows_recon(self, tmp_path):
        report = self._make_report()
        path = write_text_summary(report, tmp_path)
        content = path.read_text()
        assert "ROW COUNT" in content
        assert "1,000" in content    # formatted count

    def test_text_summary_shows_rules(self, tmp_path):
        report = self._make_report()
        path = write_text_summary(report, tmp_path)
        content = path.read_text()
        assert "BUSINESS RULES" in content
        assert "no null IDs" in content

    def test_text_summary_hipaa_notice(self, tmp_path):
        report = self._make_report()
        # Add a HIPAA-flagged rule
        report.rule_results.append(RuleResult(
            name="ssn check", rule_type="null_check",
            table="patient", column="SSN",
            passed=True, failing_count=0, total_count=500,
            fail_pct=0.0, message="HIPAA: ...",
            hipaa_flagged=True,
        ))
        report.summary.hipaa_flags = 1
        path = write_text_summary(report, tmp_path)
        content = path.read_text()
        assert "HIPAA" in content

    def test_fixture_yaml_parseable(self):
        """Smoke test: the fixture YAML can be loaded without error."""
        try:
            import yaml
        except ImportError:
            pytest.skip("PyYAML not installed")
        with open(RULES_YAML, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        assert "validations" in data
        assert len(data["validations"]) == 2

    def test_fixture_yaml_has_hipaa_table(self):
        """Fixture includes a PATIENT table with HIPAA-sensitive rules."""
        try:
            import yaml
        except ImportError:
            pytest.skip("PyYAML not installed")
        with open(RULES_YAML, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        tables = [v["target_table"] for v in data["validations"]]
        assert "patient" in tables

    def test_fixture_yaml_rule_types(self):
        try:
            import yaml
        except ImportError:
            pytest.skip("PyYAML not installed")
        with open(RULES_YAML, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        all_rule_types = {
            r["type"]
            for v in data["validations"]
            for r in v.get("rules", [])
        }
        assert "null_check" in all_rule_types
        assert "range_check" in all_rule_types
        assert "unique_check" in all_rule_types
        assert "value_set" in all_rule_types
        assert "referential" in all_rule_types
        assert "custom_sql" in all_rule_types
