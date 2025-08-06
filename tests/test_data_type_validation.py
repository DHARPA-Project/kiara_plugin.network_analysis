#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for NetworkDataType validation and parsing."""

import pyarrow as pa
import pytest

from kiara.exceptions import KiaraException
from kiara_plugin.network_analysis.data_types import NetworkDataType
from kiara_plugin.network_analysis.defaults import (
    EDGES_TABLE_NAME,
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    NODES_TABLE_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.tabular.models.tables import KiaraTables
from tests.resources.test_data import TEST_FIXTURES


class TestNetworkDataTypeBasics:
    """Test basic NetworkDataType functionality."""

    def test_data_type_name(self):
        """Test that data type name is correctly set."""
        data_type = NetworkDataType()
        assert data_type._data_type_name == "network_data"

    def test_python_class(self):
        """Test that python class returns NetworkData."""
        data_type = NetworkDataType()
        assert data_type.python_class() == NetworkData

    def test_type_doc_generation(self):
        """Test that type documentation is generated."""
        data_type = NetworkDataType()
        doc = data_type.type_doc()

        assert isinstance(doc, str)
        assert len(doc) > 0
        assert "edges" in doc.lower()
        assert "nodes" in doc.lower()
        assert "_source" in doc
        assert "_target" in doc
        assert "_node_id" in doc
        assert "_label" in doc


class TestNetworkDataTypeParsing:
    """Test NetworkDataType parsing functionality."""

    def test_parse_from_kiara_tables_valid(self):
        """Test parsing from valid KiaraTables."""
        # Create valid tables data
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
            "attribute": ["x", "y", "z"],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1, 2],
            TARGET_COLUMN_NAME: [1, 2, 0],
            "weight": [1.0, 2.0, 3.0],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        kiara_tables = KiaraTables.create_tables(
            {NODES_TABLE_NAME: nodes_table, EDGES_TABLE_NAME: edges_table}
        )

        data_type = NetworkDataType()
        result = data_type.parse_python_obj(kiara_tables)

        assert isinstance(result, NetworkData)
        assert result.num_nodes == 3
        assert result.num_edges == 3

    def test_parse_from_network_data_direct(self):
        """Test parsing from NetworkData object directly."""
        network_data = TEST_FIXTURES.simple_network_data

        data_type = NetworkDataType()
        result = data_type.parse_python_obj(network_data)

        assert result is network_data
        assert isinstance(result, NetworkData)

    def test_parse_missing_edges_table(self):
        """Test error when edges table is missing."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        nodes_table = pa.Table.from_pydict(nodes_data)

        kiara_tables = KiaraTables.create_tables(
            {
                NODES_TABLE_NAME: nodes_table,
                # Missing edges table
            }
        )

        data_type = NetworkDataType()

        with pytest.raises(KiaraException, match="no 'edges' table found"):
            data_type.parse_python_obj(kiara_tables)

    def test_parse_missing_nodes_table(self):
        """Test error when nodes table is missing."""
        edges_data = {SOURCE_COLUMN_NAME: [0], TARGET_COLUMN_NAME: [1]}
        edges_table = pa.Table.from_pydict(edges_data)

        kiara_tables = KiaraTables.create_tables(
            {
                EDGES_TABLE_NAME: edges_table,
                # Missing nodes table
            }
        )

        data_type = NetworkDataType()

        with pytest.raises(KiaraException, match="no 'nodes' table found"):
            data_type.parse_python_obj(kiara_tables)

    def test_parse_invalid_object_type(self):
        """Test error when parsing invalid object type."""
        data_type = NetworkDataType()

        with pytest.raises(KiaraException, match="invalid type"):
            data_type.parse_python_obj("invalid_string")

        with pytest.raises(KiaraException, match="invalid type"):
            data_type.parse_python_obj(123)

        with pytest.raises(KiaraException, match="invalid type"):
            data_type.parse_python_obj(["list", "object"])


class TestNetworkDataTypeValidation:
    """Test NetworkDataType validation functionality."""

    def test_validate_valid_network_data(self):
        """Test validation of valid NetworkData."""
        network_data = TEST_FIXTURES.simple_network_data
        data_type = NetworkDataType()

        # Should not raise any exceptions
        data_type._validate(network_data)

    def test_validate_invalid_type(self):
        """Test validation error for invalid type."""
        data_type = NetworkDataType()

        with pytest.raises(ValueError, match="must be of 'NetworkData'"):
            data_type._validate("not_network_data")

        with pytest.raises(ValueError, match="must be of 'NetworkData'"):
            data_type._validate(123)

    def test_validate_missing_edges_table_in_data(self):
        """Test validation error when edges table is missing from NetworkData."""
        # Create NetworkData-like object without edges table
        nodes_table = pa.Table.from_pydict(
            {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        )

        tables = KiaraTables.create_tables({NODES_TABLE_NAME: nodes_table})

        data_type = NetworkDataType()

        with pytest.raises(Exception, match="does not contain table 'edges'"):
            data_type._validate(tables)

    def test_validate_missing_nodes_table_in_data(self):
        """Test validation error when nodes table is missing from NetworkData."""
        edges_table = pa.Table.from_pydict(
            {SOURCE_COLUMN_NAME: [0], TARGET_COLUMN_NAME: [1]}
        )

        tables = KiaraTables.create_tables({EDGES_TABLE_NAME: edges_table})

        data_type = NetworkDataType()

        with pytest.raises(Exception, match="does not contain table 'nodes'"):
            data_type._validate(tables)

    def test_validate_missing_source_column(self):
        """Test validation error when source column is missing."""
        nodes_table = pa.Table.from_pydict(
            {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        )
        edges_table = pa.Table.from_pydict(
            {
                # Missing SOURCE_COLUMN_NAME
                TARGET_COLUMN_NAME: [1, 0]
            }
        )

        network_data = NetworkData.create_network_data(
            nodes_table, edges_table, augment_tables=False
        )

        data_type = NetworkDataType()

        with pytest.raises(Exception, match="does not contain a '_source' column"):
            data_type._validate(network_data)

    def test_validate_missing_target_column(self):
        """Test validation error when target column is missing."""
        nodes_table = pa.Table.from_pydict(
            {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        )
        edges_table = pa.Table.from_pydict(
            {
                SOURCE_COLUMN_NAME: [0, 1],
                # Missing TARGET_COLUMN_NAME
            }
        )

        network_data = NetworkData.create_network_data(
            nodes_table, edges_table, augment_tables=False
        )

        data_type = NetworkDataType()

        with pytest.raises(Exception, match="does not contain a '_target' column"):
            data_type._validate(network_data)

    def test_validate_missing_node_id_column(self):
        """Test validation error when node ID column is missing."""
        nodes_table = pa.Table.from_pydict(
            {
                # Missing NODE_ID_COLUMN_NAME
                LABEL_COLUMN_NAME: ["A", "B"]
            }
        )
        edges_table = pa.Table.from_pydict(
            {SOURCE_COLUMN_NAME: [0, 1], TARGET_COLUMN_NAME: [1, 0]}
        )

        network_data = NetworkData.create_network_data(
            nodes_table, edges_table, augment_tables=False
        )

        data_type = NetworkDataType()

        with pytest.raises(Exception, match="does not contain a '_node_id' column"):
            data_type._validate(network_data)

    def test_validate_missing_label_column(self):
        """Test validation error when label column is missing."""
        nodes_table = pa.Table.from_pydict(
            {
                NODE_ID_COLUMN_NAME: [0, 1],
                # Missing LABEL_COLUMN_NAME
            }
        )
        edges_table = pa.Table.from_pydict(
            {SOURCE_COLUMN_NAME: [0, 1], TARGET_COLUMN_NAME: [1, 0]}
        )

        network_data = NetworkData.create_network_data(
            nodes_table, edges_table, augment_tables=False
        )

        data_type = NetworkDataType()

        with pytest.raises(Exception, match="does not contain a '_label' column"):
            data_type._validate(network_data)


class TestNetworkDataTypePrettyPrint:
    """Test NetworkDataType pretty printing functionality."""

    def test_pretty_print_terminal_renderable(self):
        """Test terminal renderable pretty printing."""
        network_data = TEST_FIXTURES.simple_network_data

        # Create a mock Value object
        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(network_data)

        data_type = NetworkDataType()
        render_config = {"max_no_rows": 10, "max_row_height": 5, "max_cell_length": 50}

        result = data_type.pretty_print_as__terminal_renderable(value, render_config)

        # Result should be a renderable object (Group)
        assert result is not None

        # Should contain both nodes and edges sections
        result_str = str(result)
        assert "nodes" in result_str.lower() or "edges" in result_str.lower()

    def test_pretty_print_with_custom_config(self):
        """Test pretty printing with custom render configuration."""
        network_data = TEST_FIXTURES.simple_network_data

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(network_data)

        data_type = NetworkDataType()
        render_config = {
            "max_no_rows": 2,  # Very small limit
            "max_row_height": 1,
            "max_cell_length": 10,
        }

        result = data_type.pretty_print_as__terminal_renderable(value, render_config)

        assert result is not None

    def test_pretty_print_empty_config(self):
        """Test pretty printing with empty render configuration."""
        network_data = TEST_FIXTURES.simple_network_data

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(network_data)

        data_type = NetworkDataType()
        render_config = {}

        result = data_type.pretty_print_as__terminal_renderable(value, render_config)

        assert result is not None


class TestNetworkDataTypeEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_parse_empty_graph(self):
        """Test parsing of graph with no edges."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
        }
        edges_data = {SOURCE_COLUMN_NAME: [], TARGET_COLUMN_NAME: []}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        kiara_tables = KiaraTables.create_tables(
            {NODES_TABLE_NAME: nodes_table, EDGES_TABLE_NAME: edges_table}
        )

        data_type = NetworkDataType()
        result = data_type.parse_python_obj(kiara_tables)

        assert isinstance(result, NetworkData)
        assert result.num_nodes == 3
        assert result.num_edges == 0

    def test_parse_single_node(self):
        """Test parsing of graph with single node."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0], LABEL_COLUMN_NAME: ["Singleton"]}
        edges_data = {SOURCE_COLUMN_NAME: [], TARGET_COLUMN_NAME: []}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        kiara_tables = KiaraTables.create_tables(
            {NODES_TABLE_NAME: nodes_table, EDGES_TABLE_NAME: edges_table}
        )

        data_type = NetworkDataType()
        result = data_type.parse_python_obj(kiara_tables)

        assert isinstance(result, NetworkData)
        assert result.num_nodes == 1
        assert result.num_edges == 0

    def test_parse_self_loops_only(self):
        """Test parsing of graph with only self-loops."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [0, 1],  # Self-loops
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        kiara_tables = KiaraTables.create_tables(
            {NODES_TABLE_NAME: nodes_table, EDGES_TABLE_NAME: edges_table}
        )

        data_type = NetworkDataType()
        result = data_type.parse_python_obj(kiara_tables)

        assert isinstance(result, NetworkData)
        assert result.num_nodes == 2
        assert result.num_edges == 2

    def test_validate_with_extra_columns(self):
        """Test validation works with additional columns."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
            "extra_node_attr": ["x", "y", "z"],
            "numeric_attr": [1, 2, 3],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [1, 2],
            "weight": [1.0, 2.0],
            "edge_type": ["type1", "type2"],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        data_type = NetworkDataType()

        # Should validate successfully despite extra columns
        data_type._validate(network_data)

    @pytest.mark.parametrize(
        "network_name", TEST_FIXTURES.get_all_network_data().keys()
    )
    def test_validate_all_test_fixtures(self, network_name):
        """Test validation of all test fixture NetworkData instances."""
        network_data = TEST_FIXTURES.get_all_network_data()[network_name]
        data_type = NetworkDataType()

        # All test fixtures should validate successfully
        data_type._validate(network_data)

    @pytest.mark.parametrize(
        "network_name", TEST_FIXTURES.get_all_network_data().keys()
    )
    def test_parse_all_test_fixtures(self, network_name):
        """Test parsing of all test fixture NetworkData instances."""
        network_data = TEST_FIXTURES.get_all_network_data()[network_name]
        data_type = NetworkDataType()

        # Should parse successfully (identity operation)
        result = data_type.parse_python_obj(network_data)
        assert result is network_data
        assert isinstance(result, NetworkData)


class TestNetworkDataTypeDocumentation:
    """Test documentation and metadata functionality."""

    def test_type_doc_includes_column_descriptions(self):
        """Test that type documentation includes column descriptions."""
        data_type = NetworkDataType()
        doc = data_type.type_doc()

        # Should include descriptions of key columns
        expected_columns = [
            "_edge_id",
            "_source",
            "_target",
            "_node_id",
            "_label",
            "_count_edges",
        ]

        for column in expected_columns:
            assert column in doc

    def test_type_doc_includes_table_descriptions(self):
        """Test that type documentation includes table descriptions."""
        data_type = NetworkDataType()
        doc = data_type.type_doc()

        # Should have sections for edges and nodes
        assert "## Edges" in doc
        assert "## Nodes" in doc

    def test_type_doc_cached(self):
        """Test that type documentation is cached."""
        data_type = NetworkDataType()

        # Clear any existing cache
        NetworkDataType._cached_doc = None

        # First call should generate and cache
        doc1 = data_type.type_doc()
        assert NetworkDataType._cached_doc is not None

        # Second call should return cached version
        doc2 = data_type.type_doc()
        assert doc1 == doc2
        assert doc1 is doc2  # Should be the same object
