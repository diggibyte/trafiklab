"""Tests for column definitions and schema contract."""

import pytest

from htq2_gtfs.config import (
    SILVER_TABLES,
    parse_ingest_args,
    parse_quality_args,
    parse_transform_args,
)
from htq2_gtfs.processing.models import (
    JOURNEY_BASE_COLUMNS,
    JOURNEY_CALL_BASE_COLUMNS,
    SQL_VIEW_REGISTRY,
    TABLE_REGISTRY,
    generate_view_ddl,
)


class TestTableRegistry:
    """Verify the table registry matches the schema contract."""

    def test_has_22_tables(self):
        assert len(TABLE_REGISTRY) == 22

    def test_all_silver_tables_in_registry(self):
        for table_name in SILVER_TABLES:
            assert table_name in TABLE_REGISTRY, f"{table_name} missing from TABLE_REGISTRY"

    def test_journey_has_36_columns(self):
        assert len(JOURNEY_BASE_COLUMNS) == 36

    def test_journey_call_has_67_columns(self):
        assert len(JOURNEY_CALL_BASE_COLUMNS) == 67

    def test_dvjid_is_primary_key(self):
        for name, tdef in TABLE_REGISTRY.items():
            assert "DVJId" in tdef.primary_keys or "StopAreaNumber" in tdef.primary_keys, (
                f"{name} has no DVJId or StopAreaNumber in primary keys"
            )

    def test_extension_tables_have_parent(self):
        for name, tdef in TABLE_REGISTRY.items():
            if tdef.table_type == "extension":
                assert tdef.parent_table is not None, f"{name} is extension but has no parent"
                assert tdef.parent_table in TABLE_REGISTRY, f"{name} parent {tdef.parent_table} not in registry"

    def test_all_columns_are_strings(self):
        for name, tdef in TABLE_REGISTRY.items():
            for col in tdef.columns:
                assert isinstance(col, str), f"{name}.{col} is not a string"
                assert col.strip() == col, f"{name}.{col} has whitespace"


class TestSQLViews:
    """Verify SQL view definitions."""

    def test_has_14_views(self):
        assert len(SQL_VIEW_REGISTRY) == 14

    def test_view_tables_exist_in_registry(self):
        for vname, vdef in SQL_VIEW_REGISTRY.items():
            assert vdef.base_table in TABLE_REGISTRY, f"View {vname}: base {vdef.base_table} not found"
            assert vdef.extension_table in TABLE_REGISTRY, f"View {vname}: ext {vdef.extension_table} not found"

    def test_generate_view_ddl(self):
        vdef = SQL_VIEW_REGISTRY["v_journey_call_delay"]
        ddl = generate_view_ddl("htq2_dev", "silver", vdef)
        assert "CREATE OR REPLACE VIEW" in ddl
        assert "htq2_dev.silver.v_journey_call_delay" in ddl
        assert "LEFT JOIN" in ddl
        assert "ArrivalDelaySeconds" in ddl


class TestConfigParsing:
    """Verify CLI argument parsing."""

    def test_ingest_args(self):
        config = parse_ingest_args(["--catalog", "htq2_dev", "--run_mode", "both"])
        assert config.catalog == "htq2_dev"
        assert config.run_mode == "both"
        assert config.volume_path == "/Volumes/htq2_dev/bronze/trafiklab_raw"

    def test_transform_args(self):
        config = parse_transform_args(["--catalog", "htq2_staging"])
        assert config.catalog == "htq2_staging"
        assert config.silver_table("journey") == "htq2_staging.silver.journey"

    def test_quality_args(self):
        config = parse_quality_args(["--catalog", "htq2_prod"])
        assert config.table("journey") == "htq2_prod.silver.journey"
