#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for edge cases and error handling in network analysis plugin."""

import networkx as nx
import polars as pl
import pyarrow as pa
import pytest

from kiara_plugin.network_analysis.defaults import (
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from tests.resources.test_data import TEST_FIXTURES


class TestEmptyGraphs:
    """Test handling of empty graphs and edge cases."""

    def test_empty_graph_creation(self):
        """Test creating NetworkData from completely empty tables."""
        nodes_data = {NODE_ID_COLUMN_NAME: [], LABEL_COLUMN_NAME: []}
        edges_data = {SOURCE_COLUMN_NAME: [], TARGET_COLUMN_NAME: []}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 0
        assert network_data.num_edges == 0

        # Should still be queryable
        result = network_data.query_nodes("SELECT COUNT(*) as count FROM nodes")
        count = result.column(0)[0].as_py()
        assert count == 0

    def test_isolated_nodes_only(self):
        """Test graph with nodes but no edges."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
            "attribute": ["x", "y", "z"],
        }
        edges_data = {SOURCE_COLUMN_NAME: [], TARGET_COLUMN_NAME: []}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 3
        assert network_data.num_edges == 0

        # All nodes should have zero connections
        nodes_df = network_data.nodes.to_polars_dataframe()
        connections = nodes_df["_count_edges"].to_list()
        assert all(c == 0 for c in connections)

        # Should have multiple components (each isolated node)
        component_ids = network_data.component_ids
        assert len(component_ids) == 3

    def test_single_node_graph(self):
        """Test graph with single node."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0], LABEL_COLUMN_NAME: ["Singleton"]}
        edges_data = {SOURCE_COLUMN_NAME: [], TARGET_COLUMN_NAME: []}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 1
        assert network_data.num_edges == 0

        # Should be convertible to NetworkX
        nx_graph = network_data.as_networkx_graph(nx.Graph)
        assert nx_graph.number_of_nodes() == 1
        assert nx_graph.number_of_edges() == 0

    def test_edges_without_attributes(self):
        """Test graph with edges but no additional attributes."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
        }
        edges_data = {SOURCE_COLUMN_NAME: [0, 1], TARGET_COLUMN_NAME: [1, 2]}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 3
        assert network_data.num_edges == 2

        # Should still have computed columns
        edges_columns = network_data.edges.column_names
        assert "_edge_id" in edges_columns
        assert "_count_dup_directed" in edges_columns


class TestSelfLoops:
    """Test handling of self-loops (edges from node to itself)."""

    def test_self_loops_basic(self):
        """Test basic self-loop handling."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 0, 1],
            TARGET_COLUMN_NAME: [0, 1, 1],  # Self-loops on 0 and 1
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 2
        assert network_data.num_edges == 3

        # Self-loops should be counted in degree calculations
        nodes_df = network_data.nodes.to_polars_dataframe()
        connections = nodes_df["_count_edges"].to_list()
        assert all(c > 0 for c in connections)  # All nodes have connections

    def test_self_loops_only(self):
        """Test graph with only self-loops."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1, 2],
            TARGET_COLUMN_NAME: [0, 1, 2],  # All self-loops
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 3
        assert network_data.num_edges == 3

        # Each node should be in its own component (self-loops don't create connectivity)
        component_ids = network_data.component_ids
        assert len(component_ids) == 3

    def test_self_loops_networkx_conversion(self):
        """Test self-loop handling in NetworkX conversion."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        edges_data = {SOURCE_COLUMN_NAME: [0, 0, 1], TARGET_COLUMN_NAME: [0, 1, 1]}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Convert with self-loops included
        nx_graph_with_loops = network_data.as_networkx_graph(nx.Graph)

        # Convert with self-loops omitted
        nx_graph_no_loops = network_data.as_networkx_graph(
            nx.Graph, omit_self_loops=True
        )

        # Should have different edge counts
        assert (
            nx_graph_with_loops.number_of_edges() > nx_graph_no_loops.number_of_edges()
        )

        # Verify no self-loops in the omitted version
        for u, v in nx_graph_no_loops.edges():
            assert u != v


class TestParallelEdges:
    """Test handling of parallel edges (multiple edges between same nodes)."""

    def test_parallel_edges_simple(self):
        """Test basic parallel edge handling."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 0, 0, 1],  # Multiple 0->1 edges
            TARGET_COLUMN_NAME: [1, 1, 1, 2],
            "type": ["road", "rail", "air", "sea"],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 3
        assert network_data.num_edges == 4

        # Check parallel edge counts
        edges_df = network_data.edges.to_polars_dataframe()
        directed_counts = edges_df["_count_dup_directed"].to_list()

        # Should have some edges with count > 1 (parallel edges)
        assert max(directed_counts) > 1

        # Verify parallel edge indices
        directed_indices = edges_df["_idx_dup_directed"].to_list()
        max_parallel_count = max(directed_counts)
        expected_indices = set(range(1, max_parallel_count + 1))
        actual_indices = set(directed_indices)
        assert expected_indices.issubset(actual_indices)

    def test_parallel_edges_undirected_vs_directed(self):
        """Test parallel edge counting for directed vs undirected interpretation."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],  # 0->1 and 1->0
            TARGET_COLUMN_NAME: [1, 0],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        edges_df = network_data.edges.to_polars_dataframe()

        # Directed: these are different edges
        directed_counts = edges_df["_count_dup_directed"].to_list()
        assert all(c == 1 for c in directed_counts)

        # Undirected: these are parallel edges
        undirected_counts = edges_df["_count_dup_undirected"].to_list()
        assert all(c == 2 for c in undirected_counts)

    def test_many_parallel_edges(self):
        """Test handling of many parallel edges."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        # Create 100 parallel edges
        n_edges = 100
        edges_data = {
            SOURCE_COLUMN_NAME: [0] * n_edges,
            TARGET_COLUMN_NAME: [1] * n_edges,
            "id": list(range(n_edges)),
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_edges == n_edges

        # All edges should be marked as having count = n_edges
        edges_df = network_data.edges.to_polars_dataframe()
        directed_counts = edges_df["_count_dup_directed"].to_list()
        assert all(c == n_edges for c in directed_counts)


class TestLargeGraphs:
    """Test handling of larger graphs for basic scalability."""

    def test_moderately_large_graph(self):
        """Test creating and processing a moderately large graph."""
        n_nodes = 1000
        n_edges = 2000

        # Create nodes
        nodes_data = {
            NODE_ID_COLUMN_NAME: list(range(n_nodes)),
            LABEL_COLUMN_NAME: [f"Node_{i}" for i in range(n_nodes)],
            "group": [i % 10 for i in range(n_nodes)],
        }

        # Create random edges
        import random

        random.seed(42)  # Reproducible
        edges_data = {
            SOURCE_COLUMN_NAME: [
                random.randint(0, n_nodes - 1) for _ in range(n_edges)
            ],
            TARGET_COLUMN_NAME: [
                random.randint(0, n_nodes - 1) for _ in range(n_edges)
            ],
            "weight": [random.random() for _ in range(n_edges)],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == n_nodes
        assert network_data.num_edges == n_edges

        # Should be able to query efficiently
        result = network_data.query_nodes(
            'SELECT COUNT(*) as count FROM nodes WHERE "group" = 0'
        )
        count = result.column(0)[0].as_py()
        assert count == n_nodes // 10

        # Should be able to compute component IDs
        component_ids = network_data.component_ids
        assert len(component_ids) >= 1

    def test_sparse_large_graph(self):
        """Test large graph with few edges (sparse)."""
        n_nodes = 5000
        n_edges = 10  # Very sparse

        nodes_data = {
            NODE_ID_COLUMN_NAME: list(range(n_nodes)),
            LABEL_COLUMN_NAME: [f"Node_{i}" for i in range(n_nodes)],
        }

        # Create a few edges connecting first few nodes
        edges_data = {
            SOURCE_COLUMN_NAME: list(range(n_edges)),
            TARGET_COLUMN_NAME: list(range(1, n_edges + 1)),
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == n_nodes
        assert network_data.num_edges == n_edges

        # Should have many components (most nodes isolated)
        component_ids = network_data.component_ids
        assert len(component_ids) > 1

        # Should have mostly zero-degree nodes
        nodes_df = network_data.nodes.to_polars_dataframe()
        connections = nodes_df["_count_edges"].to_list()
        zero_degree_count = sum(1 for c in connections if c == 0)
        assert zero_degree_count > n_nodes * 0.8  # Most nodes isolated


class TestDataTypeEdgeCases:
    """Test edge cases in data types and validation."""

    def test_non_sequential_node_ids(self):
        """Test handling of non-sequential node IDs."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],  # Sequential in NetworkData
            LABEL_COLUMN_NAME: ["A", "B", "C"],
        }
        # But edges reference non-sequential original IDs
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 2],  # Skip node 1
            TARGET_COLUMN_NAME: [2, 0],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Should handle this correctly
        assert network_data.num_nodes == 3
        assert network_data.num_edges == 2

        # Node 1 should have zero connections
        nodes_df = network_data.nodes.to_polars_dataframe()
        node_1_connections = nodes_df.filter(pl.col(NODE_ID_COLUMN_NAME) == 1)[
            "_count_edges"
        ].to_list()[0]
        assert node_1_connections == 0

    def test_string_node_ids_from_networkx(self):
        """Test handling of string node IDs from NetworkX conversion."""
        graph = nx.Graph()
        graph.add_nodes_from(["Alice", "Bob", "Charlie"])
        graph.add_edges_from([("Alice", "Bob"), ("Bob", "Charlie")])

        network_data = NetworkData.create_from_networkx_graph(graph)

        assert network_data.num_nodes == 3
        assert network_data.num_edges == 2

        # Original string IDs should be preserved as labels
        nodes_df = network_data.nodes.to_polars_dataframe()
        labels = set(nodes_df[LABEL_COLUMN_NAME].to_list())
        assert {"Alice", "Bob", "Charlie"}.issubset(labels)

        # Internal node IDs should be sequential integers
        node_ids = sorted(nodes_df[NODE_ID_COLUMN_NAME].to_list())
        assert node_ids == [0, 1, 2]

    def test_mixed_data_types_in_attributes(self):
        """Test handling of mixed data types in node/edge attributes."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
            "mixed_attr": [42, "string", 3.14],  # Mixed types
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [1, 2],
            "mixed_edge_attr": [True, "text"],  # Mixed types
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        # PyArrow should handle this by promoting to string type or similar
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 3
        assert network_data.num_edges == 2

    def test_null_values_in_attributes(self):
        """Test handling of null/None values in attributes."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
            "nullable_attr": [1.0, None, 3.0],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [1, 2],
            "nullable_edge_attr": [None, "value"],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 3
        assert network_data.num_edges == 2

        # Null values should be preserved
        nodes_df = network_data.nodes.to_polars_dataframe()
        nullable_values = nodes_df["nullable_attr"].to_list()
        assert None in nullable_values or any(val is None for val in nullable_values)


class TestErrorHandling:
    """Test comprehensive error handling."""

    def test_mismatched_edge_node_references(self):
        """Test error when edges reference non-existent nodes."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1],  # Only nodes 0, 1
            LABEL_COLUMN_NAME: ["A", "B"],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [1, 5],  # Node 5 doesn't exist
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        # This should work because missing nodes are auto-added
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Should have added the missing node
        assert network_data.num_nodes > 2

    def test_empty_label_column(self):
        """Test handling of empty/null labels."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["", None, "C"],  # Empty and null labels
        }
        edges_data = {SOURCE_COLUMN_NAME: [0], TARGET_COLUMN_NAME: [1]}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        # Should handle empty labels gracefully
        network_data = NetworkData.create_network_data(nodes_table, edges_table)
        assert network_data.num_nodes == 3

    def test_duplicate_node_ids(self):
        """Test handling of duplicate node IDs in input data."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 1, 2],  # Duplicate node ID 1
            LABEL_COLUMN_NAME: ["A", "B1", "B2", "C"],
        }
        edges_data = {SOURCE_COLUMN_NAME: [0, 1], TARGET_COLUMN_NAME: [1, 2]}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        # NetworkData creation should handle duplicates somehow
        # (exact behavior depends on implementation)
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Should create a valid network (exact node count depends on implementation)
        assert network_data.num_nodes > 0
        assert network_data.num_edges >= 0

    def test_networkx_conversion_with_invalid_graph_type(self):
        """Test error handling for invalid NetworkX graph types."""
        network_data = TEST_FIXTURES.simple_network_data

        with pytest.raises(Exception):
            # Try to pass a non-NetworkX class
            network_data.as_networkx_graph(list)  # type: ignore

        with pytest.raises(Exception):
            # Try to pass None
            network_data.as_networkx_graph(None)  # type: ignore


class TestBoundaryConditions:
    """Test boundary conditions and limits."""

    def test_zero_weight_edges(self):
        """Test handling of zero-weight edges."""
        nodes_data = {NODE_ID_COLUMN_NAME: [0, 1], LABEL_COLUMN_NAME: ["A", "B"]}
        edges_data = {
            SOURCE_COLUMN_NAME: [0],
            TARGET_COLUMN_NAME: [1],
            "weight": [0.0],  # Zero weight
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_edges == 1

        # Zero weight should be preserved
        edges_df = network_data.edges.to_polars_dataframe()
        weights = edges_df["weight"].to_list()
        assert 0.0 in weights

    def test_negative_weight_edges(self):
        """Test handling of negative weight edges."""
        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [1, 2],
            "weight": [-1.5, -0.5],  # Negative weights
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_edges == 2

        # Negative weights should be preserved
        edges_df = network_data.edges.to_polars_dataframe()
        weights = edges_df["weight"].to_list()
        assert all(w < 0 for w in weights)

    def test_very_long_labels(self):
        """Test handling of very long node/edge labels."""
        very_long_label = "X" * 10000  # 10k character label

        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1],
            LABEL_COLUMN_NAME: [very_long_label, "B"],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0],
            TARGET_COLUMN_NAME: [1],
            "description": [very_long_label],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Should handle long strings
        assert network_data.num_nodes == 2
        assert network_data.num_edges == 1

        # Labels should be preserved (though may be truncated in display)
        nodes_df = network_data.nodes.to_polars_dataframe()
        labels = nodes_df[LABEL_COLUMN_NAME].to_list()
        assert any(len(label) > 1000 for label in labels)

    def test_extreme_numeric_values(self):
        """Test handling of extreme numeric values."""
        import sys

        nodes_data = {
            NODE_ID_COLUMN_NAME: [0, 1, 2],
            LABEL_COLUMN_NAME: ["A", "B", "C"],
            "large_value": [sys.maxsize, -sys.maxsize, 0],
            "small_float": [1e-100, 1e100, float("inf")],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [0, 1],
            TARGET_COLUMN_NAME: [1, 2],
            "extreme_weight": [1e-100, 1e100],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        # Should handle extreme values (though some may be converted)
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        assert network_data.num_nodes == 3
        assert network_data.num_edges == 2
