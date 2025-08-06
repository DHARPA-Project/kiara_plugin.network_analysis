#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the NetworkData model class."""

import networkx as nx
import pyarrow as pa
import pytest
import rustworkx as rx

from kiara.exceptions import KiaraException
from kiara_plugin.network_analysis.defaults import (
    COMPONENT_ID_COLUMN_NAME,
    CONNECTIONS_COLUMN_NAME,
    COUNT_DIRECTED_COLUMN_NAME,
    COUNT_UNDIRECTED_COLUMN_NAME,
    EDGE_ID_COLUMN_NAME,
    EDGES_TABLE_NAME,
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    NODES_TABLE_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from tests.resources.test_data import TEST_FIXTURES


class TestNetworkDataCreation:
    """Test NetworkData creation methods."""

    def test_create_network_data_basic(self):
        """Test basic NetworkData creation from PyArrow tables."""
        nodes_table, edges_table = TEST_FIXTURES.nodes_table, TEST_FIXTURES.edges_table

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data is not None
        assert network_data.num_nodes == 4
        assert network_data.num_edges == 5
        assert NODES_TABLE_NAME in network_data.table_names
        assert EDGES_TABLE_NAME in network_data.table_names

    def test_create_network_data_with_augmentation(self):
        """Test NetworkData creation with table augmentation enabled."""
        nodes_table, edges_table = TEST_FIXTURES.nodes_table, TEST_FIXTURES.edges_table

        network_data = NetworkData.create_network_data(
            nodes_table, edges_table, augment_tables=True
        )

        # Check that computed columns are present
        nodes_columns = network_data.nodes.column_names
        edges_columns = network_data.edges.column_names

        assert EDGE_ID_COLUMN_NAME in edges_columns
        assert COUNT_DIRECTED_COLUMN_NAME in edges_columns
        assert COUNT_UNDIRECTED_COLUMN_NAME in edges_columns
        assert CONNECTIONS_COLUMN_NAME in nodes_columns
        assert COMPONENT_ID_COLUMN_NAME in nodes_columns

    def test_create_network_data_without_augmentation(self):
        """Test NetworkData creation with table augmentation disabled."""
        # Create minimal tables with only required columns
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [1, 2],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(
            nodes_table, edges_table, augment_tables=False
        )

        assert network_data.num_nodes == 3
        assert network_data.num_edges == 2

        # Verify only basic columns are present
        nodes_columns = network_data.nodes.column_names
        edges_columns = network_data.edges.column_names

        assert NODE_ID_COLUMN_NAME in nodes_columns
        assert LABEL_COLUMN_NAME in nodes_columns
        assert SOURCE_COLUMN_NAME in edges_columns
        assert TARGET_COLUMN_NAME in edges_columns

    def test_create_from_networkx_graph_simple(self):
        """Test NetworkData creation from simple NetworkX graph."""
        graph = TEST_FIXTURES.simple_graph

        network_data = NetworkData.create_from_networkx_graph(graph)

        assert network_data.num_nodes == graph.number_of_nodes()
        assert network_data.num_edges == graph.number_of_edges()

        # Verify node attributes are preserved
        nodes_df = network_data.nodes.to_polars_dataframe()
        assert "type" in nodes_df.columns
        assert "age" in nodes_df.columns

        # Verify edge attributes are preserved
        edges_df = network_data.edges.to_polars_dataframe()
        assert "weight" in edges_df.columns
        assert "relationship" in edges_df.columns

    def test_create_from_networkx_graph_with_label_attr(self):
        """Test NetworkData creation with specific label attribute."""
        graph = TEST_FIXTURES.simple_graph

        network_data = NetworkData.create_from_networkx_graph(
            graph, label_attr_name="type"
        )

        nodes_df = network_data.nodes.to_polars_dataframe()
        labels = nodes_df[LABEL_COLUMN_NAME].to_list()

        # Verify labels come from the specified attribute
        assert "person" in labels
        assert "organization" in labels

    def test_create_from_networkx_graph_ignore_attributes(self):
        """Test NetworkData creation ignoring specific attributes."""
        graph = TEST_FIXTURES.simple_graph

        network_data = NetworkData.create_from_networkx_graph(
            graph, ignore_node_attributes=["age", "size"]
        )

        nodes_df = network_data.nodes.to_polars_dataframe()

        assert "type" in nodes_df.columns  # Should be preserved
        assert "age" not in nodes_df.columns  # Should be ignored
        assert "size" not in nodes_df.columns  # Should be ignored

    def test_create_augmented(self):
        """Test creating augmented NetworkData with additional columns."""
        base_network_data = TEST_FIXTURES.simple_network_data

        # Add additional node column
        additional_nodes_columns = {"importance": pa.array([0.8, 0.9, 0.7, 0.6])}

        # Add additional edge column
        additional_edges_columns = {"validated": pa.array([True, False, True, False])}

        augmented = NetworkData.create_augmented(
            base_network_data,
            additional_nodes_columns=additional_nodes_columns,
            additional_edges_columns=additional_edges_columns,
        )

        # Verify original structure is preserved
        assert augmented.num_nodes == base_network_data.num_nodes
        assert augmented.num_edges == base_network_data.num_edges

        # Verify new columns are added
        assert "importance" in augmented.nodes.column_names
        assert "validated" in augmented.edges.column_names

        # Verify original columns are still present
        assert LABEL_COLUMN_NAME in augmented.nodes.column_names
        assert "weight" in augmented.edges.column_names


class TestNetworkDataProperties:
    """Test NetworkData property access."""

    @pytest.mark.parametrize(
        "network_name", TEST_FIXTURES.get_all_network_data().keys()
    )
    def test_basic_properties(self, network_name):
        """Test basic properties of NetworkData instances."""
        network_data = TEST_FIXTURES.get_all_network_data()[network_name]

        # Properties should be non-negative integers
        assert isinstance(network_data.num_nodes, int)
        assert isinstance(network_data.num_edges, int)
        assert network_data.num_nodes >= 0
        assert network_data.num_edges >= 0

        # Tables should be accessible
        assert network_data.nodes is not None
        assert network_data.edges is not None

    def test_component_ids(self):
        """Test component ID extraction."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        component_ids = disconnected_data.component_ids

        # Should have multiple components
        assert len(component_ids) > 1
        assert all(isinstance(cid, int) for cid in component_ids)

    def test_single_component_ids(self):
        """Test component ID extraction for connected graph."""
        simple_data = TEST_FIXTURES.simple_network_data
        component_ids = simple_data.component_ids

        # Should have single component
        assert len(component_ids) == 1
        assert 0 in component_ids


class TestNetworkDataQuerying:
    """Test SQL querying functionality."""

    def test_query_nodes_basic(self):
        """Test basic node querying."""
        network_data = TEST_FIXTURES.simple_network_data

        result = network_data.query_nodes(
            f"SELECT {NODE_ID_COLUMN_NAME}, {LABEL_COLUMN_NAME} FROM nodes ORDER BY {NODE_ID_COLUMN_NAME}"
        )

        assert len(result) == network_data.num_nodes
        assert NODE_ID_COLUMN_NAME in result.column_names
        assert LABEL_COLUMN_NAME in result.column_names

    def test_query_nodes_with_filter(self):
        """Test node querying with WHERE clause."""
        network_data = TEST_FIXTURES.simple_network_data

        result = network_data.query_nodes(
            "SELECT COUNT(*) as count FROM nodes WHERE type = 'person'"
        )

        count = result.column(0)[0].as_py()
        assert isinstance(count, int)
        assert count > 0

    def test_query_edges_basic(self):
        """Test basic edge querying."""
        network_data = TEST_FIXTURES.simple_network_data

        result = network_data.query_edges(
            f"SELECT {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME} FROM edges"
        )

        assert len(result) == network_data.num_edges
        assert SOURCE_COLUMN_NAME in result.column_names
        assert TARGET_COLUMN_NAME in result.column_names

    def test_query_edges_aggregation(self):
        """Test edge querying with aggregation."""
        network_data = TEST_FIXTURES.simple_network_data

        result = network_data.query_edges("SELECT AVG(weight) as avg_weight FROM edges")

        avg_weight = result.column(0)[0].as_py()
        assert isinstance(avg_weight, (int, float))
        assert avg_weight > 0

    def test_query_with_custom_relation_name(self):
        """Test querying with custom table relation name."""
        network_data = TEST_FIXTURES.simple_network_data

        result = network_data.query_nodes(
            "SELECT COUNT(*) FROM my_nodes", relation_name="my_nodes"
        )

        count = result.column(0)[0].as_py()
        assert count == network_data.num_nodes


class TestNetworkDataConversion:
    """Test conversion to NetworkX and RustWorkX."""

    def test_as_networkx_graph(self):
        """Test conversion to NetworkX Graph."""
        network_data = TEST_FIXTURES.simple_network_data

        nx_graph = network_data.as_networkx_graph(nx.Graph)

        assert isinstance(nx_graph, nx.Graph)
        assert nx_graph.number_of_nodes() == network_data.num_nodes
        assert nx_graph.number_of_edges() == network_data.num_edges

    def test_as_networkx_digraph(self):
        """Test conversion to NetworkX DiGraph."""
        network_data = TEST_FIXTURES.digraph_network_data

        nx_digraph = network_data.as_networkx_graph(nx.DiGraph)

        assert isinstance(nx_digraph, nx.DiGraph)
        assert nx_digraph.number_of_nodes() == network_data.num_nodes

    def test_as_networkx_multigraph(self):
        """Test conversion to NetworkX MultiGraph."""
        network_data = TEST_FIXTURES.multi_network_data

        nx_multigraph = network_data.as_networkx_graph(nx.MultiGraph)

        assert isinstance(nx_multigraph, nx.MultiGraph)
        assert nx_multigraph.number_of_nodes() == network_data.num_nodes

    def test_networkx_with_node_attributes(self):
        """Test NetworkX conversion with node attributes."""
        network_data = TEST_FIXTURES.simple_network_data

        nx_graph = network_data.as_networkx_graph(nx.Graph, incl_node_attributes=True)

        # Check that node attributes are preserved
        for node_id in nx_graph.nodes():
            node_data = nx_graph.nodes[node_id]
            assert LABEL_COLUMN_NAME in node_data

    def test_networkx_with_edge_attributes(self):
        """Test NetworkX conversion with edge attributes."""
        network_data = TEST_FIXTURES.simple_network_data

        nx_graph = network_data.as_networkx_graph(nx.Graph, incl_edge_attributes=True)

        # Check that edge attributes are preserved
        for u, v in nx_graph.edges():
            edge_data = nx_graph[u][v]
            assert "weight" in edge_data

    def test_networkx_omit_self_loops(self):
        """Test NetworkX conversion omitting self-loops."""
        network_data = TEST_FIXTURES.self_loop_network_data
        original_edges = network_data.num_edges

        nx_graph = network_data.as_networkx_graph(nx.Graph, omit_self_loops=True)

        # Should have fewer edges due to omitted self-loops
        assert nx_graph.number_of_edges() < original_edges

        # Verify no self-loops exist
        for u, v in nx_graph.edges():
            assert u != v

    def test_as_rustworkx_graph(self):
        """Test conversion to RustWorkX PyGraph."""
        network_data = TEST_FIXTURES.simple_network_data

        rx_graph = network_data.as_rustworkx_graph(rx.PyGraph)

        assert isinstance(rx_graph, rx.PyGraph)
        assert rx_graph.num_nodes() == network_data.num_nodes
        assert rx_graph.num_edges() == network_data.num_edges

    def test_as_rustworkx_digraph(self):
        """Test conversion to RustWorkX PyDiGraph."""
        network_data = TEST_FIXTURES.digraph_network_data

        rx_digraph = network_data.as_rustworkx_graph(rx.PyDiGraph)

        assert isinstance(rx_digraph, rx.PyDiGraph)
        assert rx_digraph.num_nodes() == network_data.num_nodes

    def test_rustworkx_with_multigraph(self):
        """Test RustWorkX conversion with multigraph support."""
        network_data = TEST_FIXTURES.multi_network_data

        rx_graph = network_data.as_rustworkx_graph(rx.PyGraph, multigraph=True)

        assert isinstance(rx_graph, rx.PyGraph)
        assert rx_graph.multigraph is True

    def test_rustworkx_with_node_id_map(self):
        """Test RustWorkX conversion with node ID mapping."""
        network_data = TEST_FIXTURES.simple_network_data

        rx_graph = network_data.as_rustworkx_graph(rx.PyGraph, attach_node_id_map=True)

        assert hasattr(rx_graph, "attrs")
        assert "node_id_map" in rx_graph.attrs

        node_id_map = rx_graph.attrs["node_id_map"]
        assert len(node_id_map) == network_data.num_nodes


class TestNetworkDataFiltering:
    """Test NetworkData filtering operations."""

    def test_from_filtered_nodes(self):
        """Test creating filtered NetworkData from node list."""
        network_data = TEST_FIXTURES.disconnected_network_data
        original_nodes = network_data.num_nodes

        # Filter to keep only first few nodes
        nodes_to_keep = [0, 1, 2]  # Triangle component
        filtered = NetworkData.from_filtered_nodes(network_data, nodes_to_keep)

        assert filtered.num_nodes <= len(nodes_to_keep)
        assert filtered.num_nodes < original_nodes

        # Verify node IDs are remapped sequentially
        node_ids = filtered.query_nodes(
            f"SELECT {NODE_ID_COLUMN_NAME} FROM nodes ORDER BY {NODE_ID_COLUMN_NAME}"
        )[NODE_ID_COLUMN_NAME].to_pylist()

        expected_ids = list(range(filtered.num_nodes))
        assert node_ids == expected_ids

    def test_filtered_nodes_edge_consistency(self):
        """Test that filtered edges only reference remaining nodes."""
        network_data = TEST_FIXTURES.simple_network_data
        nodes_to_keep = [0, 1]  # Keep only first two nodes

        filtered = NetworkData.from_filtered_nodes(network_data, nodes_to_keep)

        # All edges should reference only the kept nodes (remapped to 0, 1)
        edges_df = filtered.edges.to_polars_dataframe()
        source_ids = set(edges_df[SOURCE_COLUMN_NAME].to_list())
        target_ids = set(edges_df[TARGET_COLUMN_NAME].to_list())

        valid_node_ids = set(range(filtered.num_nodes))
        assert source_ids.issubset(valid_node_ids)
        assert target_ids.issubset(valid_node_ids)


class TestNetworkDataErrorHandling:
    """Test error handling in NetworkData operations."""

    def test_create_with_null_source(self):
        """Test error handling with null values in source column."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        edges_data = {SOURCE_COLUMN_NAME: [None, 1], TARGET_COLUMN_NAME: [1, 0]}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        with pytest.raises(KiaraException, match="Source column.*null values"):
            NetworkData.create_network_data(nodes_table, edges_table)

    def test_create_with_null_target(self):
        """Test error handling with null values in target column."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        edges_data = {SOURCE_COLUMN_NAME: [0, 1], TARGET_COLUMN_NAME: [1, None]}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        with pytest.raises(KiaraException, match="Target column.*null values"):
            NetworkData.create_network_data(nodes_table, edges_table)

    def test_networkx_with_invalid_attribute(self):
        """Test error handling for invalid attribute names."""
        graph = nx.Graph()
        graph.add_node(0, **{"_reserved": "invalid"})

        with pytest.raises(KiaraException, match="reserved for internal use"):
            NetworkData.create_from_networkx_graph(graph)

    def test_invalid_node_attribute_inclusion(self):
        """Test error handling for non-existent node attributes."""
        network_data = TEST_FIXTURES.simple_network_data

        with pytest.raises(
            KiaraException, match="not part of the available attributes"
        ):
            network_data._calculate_node_attributes("nonexistent_attribute")

    def test_invalid_edge_attribute_inclusion(self):
        """Test error handling for non-existent edge attributes."""
        network_data = TEST_FIXTURES.simple_network_data

        with pytest.raises(
            KiaraException, match="not part of the available attributes"
        ):
            network_data._calculate_edge_attributes("nonexistent_attribute")


class TestNetworkDataCallback:
    """Test callback functionality for graph traversal."""

    def test_retrieve_graph_data_nodes_only(self):
        """Test graph data retrieval with nodes callback only."""
        network_data = TEST_FIXTURES.simple_network_data
        collected_nodes = []

        def node_callback(_node_id, _label, **kwargs):
            collected_nodes.append((_node_id, _label))

        network_data.retrieve_graph_data(
            nodes_callback=node_callback, incl_node_attributes=LABEL_COLUMN_NAME
        )

        assert len(collected_nodes) == network_data.num_nodes
        assert all(isinstance(node_id, int) for node_id, _ in collected_nodes)
        assert all(isinstance(label, str) for _, label in collected_nodes)

    def test_retrieve_graph_data_edges_only(self):
        """Test graph data retrieval with edges callback only."""
        network_data = TEST_FIXTURES.simple_network_data
        collected_edges = []

        def edge_callback(_source, _target, **kwargs):
            collected_edges.append((_source, _target))

        network_data.retrieve_graph_data(
            edges_callback=edge_callback, incl_edge_attributes=False
        )

        assert len(collected_edges) == network_data.num_edges
        assert all(isinstance(src, int) for src, _ in collected_edges)
        assert all(isinstance(tgt, int) for _, tgt in collected_edges)

    def test_retrieve_graph_data_with_attributes(self):
        """Test graph data retrieval with attribute inclusion."""
        network_data = TEST_FIXTURES.simple_network_data
        collected_nodes = []
        collected_edges = []

        def node_callback(_node_id, _label, type, **kwargs):
            collected_nodes.append((_node_id, _label, type))

        def edge_callback(_source, _target, weight, **kwargs):
            collected_edges.append((_source, _target, weight))

        network_data.retrieve_graph_data(
            nodes_callback=node_callback,
            edges_callback=edge_callback,
            incl_node_attributes=["type"],
            incl_edge_attributes=["weight"],
        )

        assert len(collected_nodes) == network_data.num_nodes
        assert len(collected_edges) == network_data.num_edges

        # Verify attribute values are included
        for node_id, label, node_type in collected_nodes:
            assert isinstance(node_type, str)

        for src, tgt, weight in collected_edges:
            assert isinstance(weight, (int, float))

    def test_retrieve_graph_data_omit_self_loops(self):
        """Test graph data retrieval omitting self-loops."""
        network_data = TEST_FIXTURES.self_loop_network_data
        collected_edges = []

        def edge_callback(_source, _target, **kwargs):
            collected_edges.append((_source, _target))

        network_data.retrieve_graph_data(
            edges_callback=edge_callback, omit_self_loops=True
        )

        # Verify no self-loops in collected edges
        for src, tgt in collected_edges:
            assert src != tgt

        # Should have fewer edges than total
        assert len(collected_edges) < network_data.num_edges
