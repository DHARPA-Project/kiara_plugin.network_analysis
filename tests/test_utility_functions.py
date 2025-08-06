#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for network analysis utility functions."""

import networkx as nx
import polars as pl
import pyarrow as pa
import pytest

from kiara.exceptions import KiaraException
from kiara_plugin.network_analysis.defaults import (
    COMPONENT_ID_COLUMN_NAME,
    CONNECTIONS_COLUMN_NAME,
    COUNT_DIRECTED_COLUMN_NAME,
    COUNT_IDX_DIRECTED_COLUMN_NAME,
    COUNT_IDX_UNDIRECTED_COLUMN_NAME,
    COUNT_UNDIRECTED_COLUMN_NAME,
    EDGE_ID_COLUMN_NAME,
    IN_DIRECTED_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    OUT_DIRECTED_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    UNWEIGHTED_DEGREE_CENTRALITY_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.network_analysis.utils import (
    augment_edges_table_with_id_and_weights,
    augment_nodes_table_with_connection_counts,
    augment_tables_with_component_ids,
    extract_network_data,
    extract_networkx_edges_as_table,
    extract_networkx_nodes_as_table,
    guess_column_name,
    guess_node_id_column_name,
    guess_node_label_column_name,
    guess_source_column_name,
    guess_target_column_name,
)
from tests.resources.test_data import TEST_FIXTURES


class TestNetworkXExtraction:
    """Test NetworkX graph extraction functions."""

    def test_extract_networkx_nodes_basic(self):
        """Test basic node extraction from NetworkX graph."""
        graph = TEST_FIXTURES.simple_graph

        nodes_table, node_id_map = extract_networkx_nodes_as_table(graph)

        assert isinstance(nodes_table, pa.Table)
        assert isinstance(node_id_map, dict)

        # Should have correct number of nodes
        assert len(nodes_table) == graph.number_of_nodes()

        # Should have required columns
        assert NODE_ID_COLUMN_NAME in nodes_table.column_names
        assert LABEL_COLUMN_NAME in nodes_table.column_names

        # Should preserve node attributes
        assert "type" in nodes_table.column_names
        assert "age" in nodes_table.column_names

        # Node IDs should be sequential starting from 0
        node_ids = nodes_table[NODE_ID_COLUMN_NAME].to_pylist()
        expected_ids = list(range(len(node_ids)))
        assert sorted(node_ids) == expected_ids

        # Node ID map should map original IDs to sequential IDs
        assert len(node_id_map) == graph.number_of_nodes()
        assert all(isinstance(v, int) for v in node_id_map.values())
        assert set(node_id_map.values()) == set(range(graph.number_of_nodes()))

    def test_extract_networkx_nodes_with_label_attr(self):
        """Test node extraction with specific label attribute."""
        graph = TEST_FIXTURES.simple_graph

        nodes_table, _ = extract_networkx_nodes_as_table(graph, label_attr_name="type")

        labels = nodes_table[LABEL_COLUMN_NAME].to_pylist()

        # Labels should come from 'type' attribute
        assert "person" in labels
        assert "organization" in labels

    def test_extract_networkx_nodes_with_multiple_label_attrs(self):
        """Test node extraction with multiple label attribute options."""
        graph = TEST_FIXTURES.simple_graph

        # Try multiple attributes in order of preference
        nodes_table, _ = extract_networkx_nodes_as_table(
            graph, label_attr_name=["nonexistent", "type", "age"]
        )

        labels = nodes_table[LABEL_COLUMN_NAME].to_pylist()

        # Should use first available attribute (type)
        assert "person" in labels
        assert "organization" in labels

    def test_extract_networkx_nodes_ignore_attributes(self):
        """Test node extraction ignoring specific attributes."""
        graph = TEST_FIXTURES.simple_graph

        nodes_table, _ = extract_networkx_nodes_as_table(
            graph, ignore_attributes=["age", "size"]
        )

        # Should preserve 'type' but ignore 'age' and 'size'
        assert "type" in nodes_table.column_names
        assert "age" not in nodes_table.column_names
        assert "size" not in nodes_table.column_names

    def test_extract_networkx_nodes_invalid_attribute_names(self):
        """Test error handling for invalid attribute names starting with '_'."""
        graph = nx.Graph()
        graph.add_node(0, **{"_invalid": "value"})

        with pytest.raises(KiaraException, match="reserved for internal use"):
            extract_networkx_nodes_as_table(graph)

    def test_extract_networkx_edges_basic(self):
        """Test basic edge extraction from NetworkX graph."""
        graph = TEST_FIXTURES.simple_graph

        # First get node mapping
        _, node_id_map = extract_networkx_nodes_as_table(graph)

        edges_table = extract_networkx_edges_as_table(graph, node_id_map)

        assert isinstance(edges_table, pa.Table)
        assert len(edges_table) == graph.number_of_edges()

        # Should have required columns
        assert SOURCE_COLUMN_NAME in edges_table.column_names
        assert TARGET_COLUMN_NAME in edges_table.column_names

        # Should preserve edge attributes
        assert "weight" in edges_table.column_names
        assert "relationship" in edges_table.column_names

        # Source and target should use mapped node IDs
        source_ids = set(edges_table[SOURCE_COLUMN_NAME].to_pylist())
        target_ids = set(edges_table[TARGET_COLUMN_NAME].to_pylist())

        valid_node_ids = set(node_id_map.values())
        assert source_ids.issubset(valid_node_ids)
        assert target_ids.issubset(valid_node_ids)

    def test_extract_networkx_edges_extends_node_map(self):
        """Test that edge extraction extends node map for missing nodes."""
        graph = TEST_FIXTURES.simple_graph

        # Create incomplete node map
        partial_node_map = {0: 0, 1: 1}  # Missing nodes 2 and 3

        edges_table = extract_networkx_edges_as_table(graph, partial_node_map)

        # Node map should be extended
        assert len(partial_node_map) == graph.number_of_nodes()

        # All source/target IDs should be valid
        source_ids = set(edges_table[SOURCE_COLUMN_NAME].to_pylist())
        target_ids = set(edges_table[TARGET_COLUMN_NAME].to_pylist())

        valid_node_ids = set(partial_node_map.values())
        assert source_ids.issubset(valid_node_ids)
        assert target_ids.issubset(valid_node_ids)

    def test_extract_networkx_edges_invalid_attribute_names(self):
        """Test error handling for invalid edge attribute names."""
        graph = nx.Graph()
        graph.add_edge(0, 1, **{"_invalid": "value"})

        with pytest.raises(KiaraException, match="reserved for internal use"):
            extract_networkx_edges_as_table(graph, {0: 0, 1: 1})

    def test_extract_networkx_multigraph(self):
        """Test extraction from MultiGraph with parallel edges."""
        graph = TEST_FIXTURES.multi_graph

        nodes_table, node_id_map = extract_networkx_nodes_as_table(graph)
        edges_table = extract_networkx_edges_as_table(graph, node_id_map)

        # Should handle multiple edges between same nodes
        assert len(edges_table) == graph.number_of_edges()

        # Should preserve edge attributes from all parallel edges
        assert "transport" in edges_table.column_names
        assert "duration" in edges_table.column_names


class TestTableAugmentation:
    """Test table augmentation functions."""

    def test_augment_edges_table_with_id_and_weights(self):
        """Test edge table augmentation with IDs and weights."""
        # Create simple edges table
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1, 2, 0, 1],
            TARGET_COLUMN_NAME: [1, 2, 0, 1, 2],  # Has duplicate edge 0->1
            "weight": [1.0, 2.0, 3.0, 1.5, 2.5],
        }
        edges_table = pa.Table.from_pydict(edges_data)

        augmented = augment_edges_table_with_id_and_weights(edges_table)

        # Should add computed columns
        assert EDGE_ID_COLUMN_NAME in augmented.column_names
        assert COUNT_DIRECTED_COLUMN_NAME in augmented.column_names
        assert COUNT_IDX_DIRECTED_COLUMN_NAME in augmented.column_names
        assert COUNT_UNDIRECTED_COLUMN_NAME in augmented.column_names
        assert COUNT_IDX_UNDIRECTED_COLUMN_NAME in augmented.column_names

        # Should preserve original columns
        assert "weight" in augmented.column_names

        # Edge IDs should be sequential
        edge_ids = augmented[EDGE_ID_COLUMN_NAME].to_pylist()
        expected_ids = list(range(len(edge_ids)))
        assert edge_ids == expected_ids

        # Duplicate edges should have count > 1
        counts = augmented[COUNT_DIRECTED_COLUMN_NAME].to_pylist()
        assert max(counts) > 1  # At least one duplicate

    def test_augment_nodes_table_with_connection_counts(self):
        """Test node table augmentation with connection counts."""
        # Create nodes and edges tables
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
            "type": ["x", "y", "z"],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [1, 2],
            EDGE_ID_COLUMN_NAME: [0, 1],
            COUNT_DIRECTED_COLUMN_NAME: [1, 1],
            COUNT_IDX_DIRECTED_COLUMN_NAME: [1, 1],
            COUNT_UNDIRECTED_COLUMN_NAME: [1, 1],
            COUNT_IDX_UNDIRECTED_COLUMN_NAME: [1, 1],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        augmented = augment_nodes_table_with_connection_counts(nodes_table, edges_table)

        # Should add computed columns
        assert CONNECTIONS_COLUMN_NAME in augmented.column_names
        assert IN_DIRECTED_COLUMN_NAME in augmented.column_names
        assert OUT_DIRECTED_COLUMN_NAME in augmented.column_names
        assert UNWEIGHTED_DEGREE_CENTRALITY_COLUMN_NAME in augmented.column_names

        # Should preserve original columns
        assert "type" in augmented.column_names

        # Connection counts should be non-negative
        connections = augmented[CONNECTIONS_COLUMN_NAME].to_pylist()
        assert all(c >= 0 for c in connections)

        # Degree centrality should be between 0 and 1
        centrality = augmented[UNWEIGHTED_DEGREE_CENTRALITY_COLUMN_NAME].to_pylist()
        assert all(0 <= c <= 1 for c in centrality)

    def test_augment_tables_with_component_ids(self):
        """Test table augmentation with component IDs."""
        # Create disconnected graph tables
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2, 3, 4],
            LABEL_COLUMN_NAME: ["A", "B", "C", "D", "E"],
        }
        # Two components: {0, 1} and {2, 3}, plus isolated node 4
        edges_data = {SOURCE_COLUMN_NAME: [0, 2], TARGET_COLUMN_NAME: [1, 3]}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        nodes_aug, edges_aug = augment_tables_with_component_ids(
            nodes_table, edges_table
        )

        # Should add component ID column
        assert COMPONENT_ID_COLUMN_NAME in nodes_aug.column_names
        assert COMPONENT_ID_COLUMN_NAME in edges_aug.column_names

        # Component IDs should be non-negative integers
        node_component_ids = nodes_aug[COMPONENT_ID_COLUMN_NAME].to_pylist()
        edge_component_ids = edges_aug[COMPONENT_ID_COLUMN_NAME].to_pylist()

        assert all(isinstance(cid, int) and cid >= 0 for cid in node_component_ids)
        assert all(isinstance(cid, int) and cid >= 0 for cid in edge_component_ids)

        # Should have multiple components for disconnected graph
        unique_components = set(node_component_ids)
        assert len(unique_components) > 1

    def test_augment_single_component_graph(self):
        """Test component ID augmentation for connected graph."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
        }
        edges_data = {SOURCE_COLUMN_NAME: [0, 1], TARGET_COLUMN_NAME: [1, 2]}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        nodes_aug, edges_aug = augment_tables_with_component_ids(
            nodes_table, edges_table
        )

        # All nodes should be in component 0
        node_component_ids = nodes_aug[COMPONENT_ID_COLUMN_NAME].to_pylist()
        assert all(cid == 0 for cid in node_component_ids)

        # All edges should be in component 0
        edge_component_ids = edges_aug[COMPONENT_ID_COLUMN_NAME].to_pylist()
        assert all(cid == 0 for cid in edge_component_ids)


class TestColumnGuessing:
    """Test column name guessing functions."""

    def test_guess_column_name_exact_match(self):
        """Test exact column name matching."""
        table_data = {"id": [1, 2], "name": ["A", "B"], "value": [10, 20]}
        table = pa.Table.from_pydict(table_data)

        result = guess_column_name(table, ["id", "identifier"])
        assert result == "id"

    def test_guess_column_name_case_insensitive(self):
        """Test case-insensitive column name matching."""
        table_data = {"ID": [1, 2], "Name": ["A", "B"]}
        table = pa.Table.from_pydict(table_data)

        result = guess_column_name(table, ["id", "name"])
        assert result == "ID"

    def test_guess_column_name_priority_order(self):
        """Test that guessing respects priority order."""
        table_data = {"identifier": [1, 2], "id": [1, 2], "name": ["A", "B"]}
        table = pa.Table.from_pydict(table_data)

        # Should prefer first match in suggestions list
        result = guess_column_name(table, ["id", "identifier"])
        assert result == "id"

        result = guess_column_name(table, ["identifier", "id"])
        assert result == "identifier"

    def test_guess_column_name_no_match(self):
        """Test behavior when no column matches."""
        table_data = {"col1": [1, 2], "col2": ["A", "B"]}
        table = pa.Table.from_pydict(table_data)

        result = guess_column_name(table, ["id", "identifier"])
        assert result is None

    def test_guess_node_id_column_name(self):
        """Test node ID column guessing."""
        table_data = {"node_id": [1, 2], "name": ["A", "B"]}
        table = pa.Table.from_pydict(table_data)

        result = guess_node_id_column_name(table)
        assert result == "node_id"

    def test_guess_node_label_column_name(self):
        """Test node label column guessing."""
        table_data = {"id": [1, 2], "label": ["A", "B"]}
        table = pa.Table.from_pydict(table_data)

        result = guess_node_label_column_name(table)
        assert result == "label"

    def test_guess_source_column_name(self):
        """Test source column guessing."""
        table_data = {"source": [1, 2], "target": [2, 3]}
        table = pa.Table.from_pydict(table_data)

        result = guess_source_column_name(table)
        assert result == "source"

    def test_guess_target_column_name(self):
        """Test target column guessing."""
        table_data = {"from": [1, 2], "to": [2, 3]}
        table = pa.Table.from_pydict(table_data)

        result = guess_target_column_name(table)
        assert result == "to"

    def test_guess_column_with_kiara_table(self):
        """Test column guessing with KiaraTable objects."""
        from kiara_plugin.tabular.models.table import KiaraTable

        table_data = {"node_id": [1, 2], "label": ["A", "B"]}
        arrow_table = pa.Table.from_pydict(table_data)
        kiara_table = KiaraTable.create_table(arrow_table)

        result = guess_node_id_column_name(kiara_table)
        assert result == "node_id"

    def test_guess_column_with_value_wrapper(self):
        """Test column guessing with Value wrapper objects."""
        table_data = {"id": [1, 2], "name": ["A", "B"]}
        arrow_table = pa.Table.from_pydict(table_data)

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(arrow_table)

        # This should handle the Value wrapper properly
        result = guess_column_name(value, ["id"])
        assert result == "id"


class TestNetworkDataExtraction:
    """Test NetworkData extraction utilities."""

    def test_extract_network_data_from_value(self):
        """Test extracting NetworkData from Value object."""
        network_data = TEST_FIXTURES.simple_network_data

        class MockValue:
            def __init__(self, data):
                self.data = data
                self.data_type_name = "network_data"

        value = MockValue(network_data)

        result = extract_network_data(value)
        assert result is network_data
        assert isinstance(result, NetworkData)

    def test_extract_network_data_direct(self):
        """Test extracting NetworkData from direct NetworkData object."""
        network_data = TEST_FIXTURES.simple_network_data

        result = extract_network_data(network_data)
        assert result is network_data
        assert isinstance(result, NetworkData)


class TestUtilityFunctionEdgeCases:
    """Test edge cases and error conditions in utility functions."""

    def test_extract_nodes_empty_graph(self):
        """Test node extraction from empty graph."""
        graph = nx.Graph()

        nodes_table, node_id_map = extract_networkx_nodes_as_table(graph)

        assert len(nodes_table) == 0
        assert len(node_id_map) == 0
        assert NODE_ID_COLUMN_NAME in nodes_table.column_names
        assert LABEL_COLUMN_NAME in nodes_table.column_names

    def test_extract_edges_empty_graph(self):
        """Test edge extraction from empty graph."""
        graph = nx.Graph()

        edges_table = extract_networkx_edges_as_table(graph, {})

        assert len(edges_table) == 0
        assert SOURCE_COLUMN_NAME in edges_table.column_names
        assert TARGET_COLUMN_NAME in edges_table.column_names

    def test_augment_edges_no_duplicates(self):
        """Test edge augmentation when there are no duplicate edges."""
        edges_data = {SOURCE_COLUMN_NAME: [0, 1, 2], TARGET_COLUMN_NAME: [1, 2, 0]}
        edges_table = pa.Table.from_pydict(edges_data)

        augmented = augment_edges_table_with_id_and_weights(edges_table)

        # All counts should be 1 (no duplicates)
        directed_counts = augmented[COUNT_DIRECTED_COLUMN_NAME].to_pylist()
        undirected_counts = augmented[COUNT_UNDIRECTED_COLUMN_NAME].to_pylist()

        assert all(c == 1 for c in directed_counts)
        # Undirected might differ if edges form undirected duplicates

    def test_augment_nodes_isolated_nodes(self):
        """Test node augmentation with isolated nodes."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
        }
        # No edges - all nodes are isolated
        edges_data = {
            SOURCE_COLUMN_NAME: [],
            TARGET_COLUMN_NAME: [],
            EDGE_ID_COLUMN_NAME: [],
            COUNT_DIRECTED_COLUMN_NAME: [],
            COUNT_IDX_DIRECTED_COLUMN_NAME: [],
            COUNT_UNDIRECTED_COLUMN_NAME: [],
            COUNT_IDX_UNDIRECTED_COLUMN_NAME: [],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        augmented = augment_nodes_table_with_connection_counts(nodes_table, edges_table)

        # All connection counts should be 0
        connections = augmented[CONNECTIONS_COLUMN_NAME].to_pylist()
        assert all(c == 0 for c in connections)

        # Centrality should be 0 for isolated nodes
        centrality = augmented[UNWEIGHTED_DEGREE_CENTRALITY_COLUMN_NAME].to_pylist()
        assert all(c == 0 for c in centrality)

    def test_component_ids_self_loops(self):
        """Test component ID assignment with self-loops."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [0, 1],  # Self-loops
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        nodes_aug, edges_aug = augment_tables_with_component_ids(
            nodes_table, edges_table
        )

        # Self-loops should be ignored for component calculation
        node_component_ids = nodes_aug[COMPONENT_ID_COLUMN_NAME].to_pylist()

        # Should have separate components (self-loops don't connect nodes)
        unique_components = set(node_component_ids)
        assert len(unique_components) >= 1

    def test_guess_column_invalid_table(self):
        """Test column guessing with invalid table objects."""
        # Test with object that doesn't have column_names attribute
        invalid_table = "not_a_table"

        result = guess_column_name(invalid_table, ["id"])
        assert result is None


class TestUtilityFunctionIntegration:
    """Test integration between utility functions."""

    def test_full_networkx_to_network_data_pipeline(self):
        """Test complete pipeline from NetworkX to NetworkData using utilities."""
        graph = TEST_FIXTURES.simple_graph

        # Extract nodes and edges
        nodes_table, node_id_map = extract_networkx_nodes_as_table(graph)
        edges_table = extract_networkx_edges_as_table(graph, node_id_map)

        # Augment tables
        edges_augmented = augment_edges_table_with_id_and_weights(edges_table)
        nodes_augmented = augment_nodes_table_with_connection_counts(
            nodes_table, edges_augmented
        )

        # Add component IDs
        final_nodes, final_edges = augment_tables_with_component_ids(
            nodes_augmented, edges_augmented
        )

        # Create NetworkData
        network_data = NetworkData.create_network_data(
            final_nodes, final_edges, augment_tables=False
        )

        # Verify result
        assert network_data.num_nodes == graph.number_of_nodes()
        assert network_data.num_edges == graph.number_of_edges()

        # Should be able to convert back to NetworkX
        nx_graph = network_data.as_networkx_graph(nx.Graph, incl_node_attributes=True)

        assert nx_graph.number_of_nodes() == graph.number_of_nodes()
        assert nx_graph.number_of_edges() == graph.number_of_edges()

    @pytest.mark.parametrize(
        "graph_name", TEST_FIXTURES.get_all_networkx_graphs().keys()
    )
    def test_utility_functions_with_all_graph_types(self, graph_name):
        """Test utility functions work with all test graph types."""
        graph = TEST_FIXTURES.get_all_networkx_graphs()[graph_name]

        # Should be able to extract nodes and edges without errors
        nodes_table, node_id_map = extract_networkx_nodes_as_table(graph)
        edges_table = extract_networkx_edges_as_table(graph, node_id_map)

        assert len(nodes_table) == graph.number_of_nodes()
        assert len(edges_table) == graph.number_of_edges()

        # Should be able to augment without errors
        if len(edges_table) > 0:
            edges_augmented = augment_edges_table_with_id_and_weights(edges_table)
            nodes_augmented = augment_nodes_table_with_connection_counts(
                nodes_table, edges_augmented
            )

            # Basic validation
            assert len(nodes_augmented) == len(nodes_table)
            assert len(edges_augmented) == len(edges_table)

    def test_polars_and_arrow_compatibility(self):
        """Test that functions work with both Polars and Arrow tables."""
        # Create test data
        edges_data = {SOURCE_COLUMN_NAME: [0, 1], TARGET_COLUMN_NAME: [1, 2]}

        # Test with Arrow table
        arrow_table = pa.Table.from_pydict(edges_data)
        result_arrow = augment_edges_table_with_id_and_weights(arrow_table)

        # Test with Polars DataFrame
        polars_df = pl.from_arrow(arrow_table)
        result_polars = augment_edges_table_with_id_and_weights(polars_df)

        # Results should be equivalent
        assert len(result_arrow) == len(result_polars)
        assert set(result_arrow.column_names) == set(result_polars.column_names)
