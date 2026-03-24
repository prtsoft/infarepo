"""
Tests for pc_extractor.lineage — connector chain traversal and lookup detection.

Fixtures
--------
m_sample_straight_move.xml   — SQ(INPUT/OUTPUT ports) → Expression → Target
                                Tests the canonical straight-move path from the
                                reference Java implementation (infa-s2t-gen).
m_lineage_lookup.xml         — SQ(OUTPUT-only ports) → Expression + connected
                                lookup + unconnected :LKP. lookup → Target
sample_export.xml            — M_LOAD_FACT_ORDERS: SQ(OUTPUT) → Expression
                                → Filter → Target with a SYSDATE derived field
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))

from pc_extractor.xml_parser import parse_xml_file
from pc_extractor.lineage import trace_mapping
from pc_extractor.models import MappingLineage

FIXTURES = Path(__file__).parent / "fixtures"
STRAIGHT_MOVE_XML  = FIXTURES / "m_sample_straight_move.xml"
LOOKUP_XML         = FIXTURES / "m_lineage_lookup.xml"
SAMPLE_EXPORT_XML  = FIXTURES / "sample_export.xml"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _field(lineage: MappingLineage, tgt_field: str):
    """Return the FieldLineage for a given target field name."""
    return next((f for f in lineage.fields if f.target_field == tgt_field), None)


# ---------------------------------------------------------------------------
# Straight-move fixture (mirrors infa-s2t-gen MappingTest)
# ---------------------------------------------------------------------------

class TestStraightMove:
    @pytest.fixture(scope="class")
    def lineage(self):
        _, folders = parse_xml_file(STRAIGHT_MOVE_XML)
        folder = folders["TEST_FOLDER"]
        mapping = folder.mappings["m_sample_straight_move"]
        return trace_mapping(mapping, folder.sources, folder.targets)

    def test_mapping_name(self, lineage):
        assert lineage.mapping_name == "m_sample_straight_move"

    def test_one_target_field(self, lineage):
        assert len(lineage.fields) == 1
        assert lineage.fields[0].target_field == "ID"

    def test_source_is_dual(self, lineage):
        fl = _field(lineage, "ID")
        assert len(fl.sources) == 1
        assert fl.sources[0].table == "DUAL"
        assert fl.sources[0].field == "DUMMY"

    def test_source_field_type_enriched(self, lineage):
        fl = _field(lineage, "ID")
        assert fl.sources[0].field_type == "varchar2"

    def test_chain_passes_through_expression(self, lineage):
        fl = _field(lineage, "ID")
        transform_types = [n.transform_type for n in fl.chain]
        assert "Expression" in transform_types

    def test_chain_includes_source_node(self, lineage):
        fl = _field(lineage, "ID")
        assert fl.chain[-1].transform_type == "Source Definition"
        assert fl.chain[-1].instance == "DUAL"

    def test_no_lookups(self, lineage):
        fl = _field(lineage, "ID")
        assert fl.lookups == []
        assert not fl.has_unconnected_lookup


# ---------------------------------------------------------------------------
# sample_export.xml — M_LOAD_FACT_ORDERS
# Covers: Expression INPUT/OUTPUT + Filter + SQ with OUTPUT-only ports
# ---------------------------------------------------------------------------

class TestLoadFactOrders:
    @pytest.fixture(scope="class")
    def lineage(self):
        _, folders = parse_xml_file(SAMPLE_EXPORT_XML)
        folder = folders["SALES_MART"]
        mapping = folder.mappings["M_LOAD_FACT_ORDERS"]
        return trace_mapping(mapping, folder.sources, folder.targets)

    def test_order_id_traces_to_source(self, lineage):
        fl = _field(lineage, "ORDER_ID")
        assert fl is not None
        assert any(s.table == "SRC_ORDERS" and s.field == "ORDER_ID" for s in fl.sources)

    def test_customer_id_traces_to_source(self, lineage):
        fl = _field(lineage, "CUSTOMER_ID")
        assert fl is not None
        assert any(s.table == "SRC_ORDERS" for s in fl.sources)

    def test_order_amt_traces_to_source(self, lineage):
        fl = _field(lineage, "ORDER_AMT")
        assert fl is not None
        assert len(fl.sources) == 1
        assert fl.sources[0].table == "SRC_ORDERS"

    def test_load_ts_has_no_source(self, lineage):
        # LOAD_TS = SYSDATE — derived constant, no upstream connector
        fl = _field(lineage, "LOAD_TS")
        assert fl is not None
        assert fl.sources == []

    def test_load_ts_expression_captured(self, lineage):
        fl = _field(lineage, "LOAD_TS")
        assert fl.expression == "SYSDATE"

    def test_chain_traverses_filter(self, lineage):
        fl = _field(lineage, "ORDER_ID")
        trf_types = {n.transform_type for n in fl.chain}
        assert "Filter" in trf_types

    def test_chain_traverses_expression(self, lineage):
        fl = _field(lineage, "ORDER_ID")
        trf_types = {n.transform_type for n in fl.chain}
        assert "Expression" in trf_types

    def test_unconnected_target_field_has_no_sources(self, lineage):
        # CUST_NAME is in TGT_FACT_ORDERS but no connector feeds it
        fl = _field(lineage, "CUST_NAME")
        assert fl is not None
        assert fl.sources == []

    def test_no_lookups_in_simple_mapping(self, lineage):
        for fl in lineage.fields:
            assert fl.lookups == []


# ---------------------------------------------------------------------------
# m_lineage_lookup.xml — connected lookup + unconnected lookup
# ---------------------------------------------------------------------------

class TestConnectedLookup:
    @pytest.fixture(scope="class")
    def lineage(self):
        _, folders = parse_xml_file(LOOKUP_XML)
        folder = folders["TEST_FOLDER"]
        mapping = folder.mappings["m_lineage_lookup"]
        return trace_mapping(mapping, folder.sources, folder.targets)

    def test_order_id_traces_to_source(self, lineage):
        fl = _field(lineage, "ORDER_ID")
        assert fl is not None
        assert len(fl.sources) == 1
        assert fl.sources[0].table == "SRC_ORDERS"
        assert fl.sources[0].field == "ORDER_ID"

    def test_region_has_lookup_annotation(self, lineage):
        fl = _field(lineage, "REGION")
        assert fl is not None
        # Should have at least one lookup recorded
        assert len(fl.lookups) >= 1
        lkp = next((l for l in fl.lookups if l.lookup_name == "LKP_REGION"), None)
        assert lkp is not None
        assert lkp.is_connected is True
        assert lkp.lookup_table == "SRC_REGION"
        assert "CUSTOMER_ID" in lkp.lookup_condition

    def test_region_traces_back_to_source_via_lookup(self, lineage):
        # REGION ← LKP_REGION(IN_CUST_ID) ← EXP_ENRICH.CUSTOMER_ID ← SQ ← SRC_ORDERS
        fl = _field(lineage, "REGION")
        assert fl is not None
        assert any(s.table == "SRC_ORDERS" for s in fl.sources)

    def test_tax_code_has_unconnected_lookup(self, lineage):
        fl = _field(lineage, "TAX_CODE")
        assert fl is not None
        assert fl.has_unconnected_lookup is True

    def test_tax_code_unconnected_lookup_name(self, lineage):
        fl = _field(lineage, "TAX_CODE")
        lkp = next((l for l in fl.lookups if l.lookup_name == "LKP_TAX_CODE"), None)
        assert lkp is not None
        assert lkp.is_connected is False
        assert lkp.lookup_table == "TAX_CODES"

    def test_tax_code_expression_captured(self, lineage):
        fl = _field(lineage, "TAX_CODE")
        assert ":LKP.LKP_TAX_CODE" in fl.expression

    def test_load_ts_is_sysdate_no_source(self, lineage):
        fl = _field(lineage, "LOAD_TS")
        assert fl is not None
        assert fl.sources == []
        assert fl.expression == "SYSDATE"

    def test_sq_output_only_port_resolved(self, lineage):
        # SQ_ORDERS uses OUTPUT-only ports — our fallback path must handle this
        fl = _field(lineage, "ORDER_ID")
        assert fl is not None
        # The chain should NOT terminate at SQ — it should reach the source
        trf_types = [n.transform_type for n in fl.chain]
        assert "Source Definition" in trf_types


# ---------------------------------------------------------------------------
# Cycle / edge case guards
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_mapping_returns_empty_lineage(self):
        from pc_extractor.models import MappingDef
        m = MappingDef(name="EMPTY", folder="F")
        lineage = trace_mapping(m)
        assert lineage.fields == []

    def test_unconnected_target_field_no_crash(self):
        """Target field with no inbound connector should yield empty sources, not crash."""
        _, folders = parse_xml_file(SAMPLE_EXPORT_XML)
        folder = folders["SALES_MART"]
        mapping = folder.mappings["M_LOAD_FACT_ORDERS"]
        lineage = trace_mapping(mapping, folder.sources, folder.targets)
        # REGION has no connector in M_LOAD_FACT_ORDERS
        fl = _field(lineage, "REGION")
        assert fl is not None
        assert fl.sources == []
        assert fl.chain == []
