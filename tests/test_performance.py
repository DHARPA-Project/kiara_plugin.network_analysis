#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Performance and scalability tests for network analysis plugin."""

import random
import time
from typing import Tuple

import networkx as nx
import pyarrow as pa
import pytest

from kiara_plugin.network_analysis.defaults import (
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData


class TestNetworkDataPerformance:
    """Test performance characteristics of NetworkData operations."""

    def create_test_graph_data(
        self, n_nodes: int, n_edges: int, seed: int = 42
    ) -> Tuple[pa.Table, pa.Table]:
        """Create test graph data of specified size."""
        random.seed(seed)

        # Create nodes
        nodes_data = {
            NODE_ID_COLUMN_NAME: list(range(n_nodes)),
            LABEL_COLUMN_NAME: [f"Node_{i}" for i in range(n_nodes)],
            "group": [i % 10 for i in range(n_nodes)],
            "weight": [random.uniform(0, 100) for _ in range(n_nodes)],
        }

        # Create edges
        edges_data = {
            SOURCE_COLUMN_NAME: [
                random.randint(0, n_nodes - 1) for _ in range(n_edges)
            ],
            TARGET_COLUMN_NAME: [
                random.randint(0, n_nodes - 1) for _ in range(n_edges)
            ],
            "weight": [random.uniform(0, 10) for _ in range(n_edges)],
            "type": [f"type_{random.randint(0, 4)}" for _ in range(n_edges)],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        return nodes_table, edges_table

    def test_medium_graph_creation_performance(self):
        """Test performance of creating medium-sized NetworkData."""
        n_nodes, n_edges = 5000, 10000

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)

        start_time = time.time()
        network_data = NetworkData.create_network_data(nodes_table, edges_table)
        creation_time = time.time() - start_time

        # Should complete reasonably quickly (adjust threshold as needed)
        assert creation_time < 30.0  # 30 seconds max
        assert network_data.num_nodes == n_nodes
        assert network_data.num_edges == n_edges

        print(f"Created {n_nodes} nodes, {n_edges} edges in {creation_time:.2f}s")

    def test_large_graph_creation_performance(self):
        """Test performance of creating large NetworkData."""
        n_nodes, n_edges = 10000, 50000

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)

        start_time = time.time()
        network_data = NetworkData.create_network_data(nodes_table, edges_table)
        creation_time = time.time() - start_time

        # Should complete within reasonable time (adjust as needed)
        assert creation_time < 60.0  # 60 seconds max
        assert network_data.num_nodes == n_nodes
        assert network_data.num_edges == n_edges

        print(f"Created {n_nodes} nodes, {n_edges} edges in {creation_time:.2f}s")

    def test_query_performance_on_large_graph(self):
        """Test query performance on large graphs."""
        n_nodes, n_edges = 10000, 20000

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Test node queries
        start_time = time.time()
        result = network_data.query_nodes(
            'SELECT COUNT(*) as count FROM nodes WHERE "group" = 0'
        )
        query_time = time.time() - start_time

        assert query_time < 5.0  # Should be fast
        count = result.column(0)[0].as_py()
        assert count > 0

        # Test edge queries
        start_time = time.time()
        result = network_data.query_edges(
            "SELECT AVG(weight) as avg_weight FROM edges WHERE weight > 5.0"
        )
        query_time = time.time() - start_time

        assert query_time < 5.0  # Should be fast
        avg_weight = result.column(0)[0].as_py()
        assert avg_weight is not None

        print(f"Query performance: node and edge queries < {query_time:.2f}s each")

    def test_networkx_conversion_performance(self):
        """Test performance of NetworkX conversion."""
        n_nodes, n_edges = 5000, 10000

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Test conversion to NetworkX Graph
        start_time = time.time()
        nx_graph = network_data.as_networkx_graph(nx.Graph)
        conversion_time = time.time() - start_time

        assert conversion_time < 30.0  # Should complete reasonably quickly
        assert nx_graph.number_of_nodes() == n_nodes

        print(
            f"NetworkX conversion: {n_nodes} nodes, {n_edges} edges in {conversion_time:.2f}s"
        )

    def test_rustworkx_conversion_performance(self):
        """Test performance of RustWorkX conversion."""
        n_nodes, n_edges = 5000, 10000

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Test conversion to RustWorkX
        import rustworkx as rx

        start_time = time.time()
        rx_graph = network_data.as_rustworkx_graph(rx.PyGraph)
        conversion_time = time.time() - start_time

        assert conversion_time < 30.0  # Should complete reasonably quickly
        assert rx_graph.num_nodes() == n_nodes

        print(
            f"RustWorkX conversion: {n_nodes} nodes, {n_edges} edges in {conversion_time:.2f}s"
        )

    def test_component_analysis_performance(self):
        """Test performance of component analysis."""
        n_nodes, n_edges = 5000, 8000  # Sparse graph likely to have multiple components

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        start_time = time.time()
        component_ids = network_data.component_ids
        component_time = time.time() - start_time

        assert component_time < 15.0  # Should be reasonably fast
        assert len(component_ids) > 0

        print(
            f"Component analysis: {n_nodes} nodes in {component_time:.2f}s, found {len(component_ids)} components"
        )


class TestMemoryUsage:
    """Test memory usage characteristics."""

    @pytest.mark.skip(reason="Memory testing requires special setup")
    def test_memory_usage_scaling(self):
        """Test that memory usage scales reasonably with graph size."""
        import os

        import psutil

        process = psutil.Process(os.getpid())

        sizes = [(1000, 2000), (2000, 4000), (4000, 8000)]
        memory_usages = []

        for n_nodes, n_edges in sizes:
            # Measure baseline memory
            baseline_memory = process.memory_info().rss

            # Create graph
            nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)
            network_data = NetworkData.create_network_data(nodes_table, edges_table)

            # Measure memory after creation
            after_memory = process.memory_info().rss
            memory_usage = after_memory - baseline_memory
            memory_usages.append(memory_usage)

            print(f"Graph {n_nodes}x{n_edges}: {memory_usage / 1024 / 1024:.1f} MB")

            # Clean up
            del network_data
            del nodes_table
            del edges_table

        # Memory usage should scale roughly linearly
        # (This is a very rough check - exact scaling depends on many factors)
        assert memory_usages[1] > memory_usages[0]  # Larger graph uses more memory
        assert (
            memory_usages[2] > memory_usages[1]
        )  # Even larger graph uses even more memory


class TestScalabilityLimits:
    """Test behavior at various scale limits."""

    def test_dense_graph_performance(self):
        """Test performance with dense graphs (many edges relative to nodes)."""
        n_nodes = 1000
        n_edges = 50000  # Dense graph

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)

        start_time = time.time()
        network_data = NetworkData.create_network_data(nodes_table, edges_table)
        creation_time = time.time() - start_time

        # Dense graphs may take longer but should still complete
        assert creation_time < 60.0
        assert network_data.num_nodes == n_nodes
        assert network_data.num_edges == n_edges

        print(f"Dense graph {n_nodes} nodes, {n_edges} edges in {creation_time:.2f}s")

    def test_sparse_graph_performance(self):
        """Test performance with sparse graphs (few edges relative to nodes)."""
        n_nodes = 10000
        n_edges = 5000  # Sparse graph

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)

        start_time = time.time()
        network_data = NetworkData.create_network_data(nodes_table, edges_table)
        creation_time = time.time() - start_time

        # Sparse graphs should be efficient
        assert creation_time < 30.0
        assert network_data.num_nodes == n_nodes
        assert network_data.num_edges == n_edges

        print(f"Sparse graph {n_nodes} nodes, {n_edges} edges in {creation_time:.2f}s")

    def test_many_attributes_performance(self):
        """Test performance with many node/edge attributes."""
        n_nodes, n_edges = 2000, 4000
        n_attributes = 50  # Many attributes

        # Create nodes with many attributes
        nodes_data = {NODE_ID_COLUMN_NAME: list(range(n_nodes))}
        nodes_data[LABEL_COLUMN_NAME] = [f"Node_{i}" for i in range(n_nodes)]

        for i in range(n_attributes):
            nodes_data[f"attr_{i}"] = [random.uniform(0, 100) for _ in range(n_nodes)]

        # Create edges with many attributes
        edges_data = {
            SOURCE_COLUMN_NAME: [
                random.randint(0, n_nodes - 1) for _ in range(n_edges)
            ],
            TARGET_COLUMN_NAME: [
                random.randint(0, n_nodes - 1) for _ in range(n_edges)
            ],
        }

        for i in range(n_attributes):
            edges_data[f"edge_attr_{i}"] = [
                random.uniform(0, 10) for _ in range(n_edges)
            ]

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        start_time = time.time()
        network_data = NetworkData.create_network_data(nodes_table, edges_table)
        creation_time = time.time() - start_time

        # Should handle many attributes reasonably
        assert creation_time < 45.0
        assert len(network_data.nodes.column_names) > n_attributes
        assert len(network_data.edges.column_names) > n_attributes

        print(f"Many attributes: {n_attributes} attrs/table in {creation_time:.2f}s")


class TestIterativeOperations:
    """Test performance of iterative operations."""

    def test_repeated_queries_performance(self):
        """Test performance of repeated queries on same graph."""
        n_nodes, n_edges = 5000, 10000

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Perform many queries
        n_queries = 100

        start_time = time.time()
        for i in range(n_queries):
            group_id = i % 10
            result = network_data.query_nodes(
                f'SELECT COUNT(*) FROM nodes WHERE "group" = {group_id}'
            )
            count = result.column(0)[0].as_py()
            assert count >= 0

        total_time = time.time() - start_time
        avg_time = total_time / n_queries

        assert avg_time < 0.1  # Average query should be fast
        print(f"Repeated queries: {n_queries} queries, avg {avg_time:.3f}s each")

    def test_multiple_conversions_performance(self):
        """Test performance of multiple format conversions."""
        n_nodes, n_edges = 2000, 4000

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        # Test multiple NetworkX conversions
        start_time = time.time()
        for _ in range(10):
            nx_graph = network_data.as_networkx_graph(nx.Graph)
            assert nx_graph.number_of_nodes() == n_nodes

        nx_conversion_time = time.time() - start_time

        # Test multiple RustWorkX conversions
        import rustworkx as rx

        start_time = time.time()
        for _ in range(10):
            rx_graph = network_data.as_rustworkx_graph(rx.PyGraph)
            assert rx_graph.num_nodes() == n_nodes

        rx_conversion_time = time.time() - start_time

        # Conversions should be reasonable (caching might help)
        assert nx_conversion_time < 30.0
        assert rx_conversion_time < 30.0

        print(
            f"Multiple conversions: 10x NetworkX in {nx_conversion_time:.2f}s, 10x RustWorkX in {rx_conversion_time:.2f}s"
        )


class TestConcurrencyBehavior:
    """Test behavior under concurrent access (basic thread safety)."""

    def test_concurrent_queries(self):
        """Test concurrent queries on same NetworkData instance."""
        import threading
        import time

        n_nodes, n_edges = 3000, 6000

        nodes_table, edges_table = self.create_test_graph_data(n_nodes, n_edges)
        network_data = NetworkData.create_network_data(nodes_table, edges_table)

        results = []
        errors = []

        def query_worker(thread_id: int):
            """Worker function for concurrent queries."""
            try:
                for i in range(10):
                    result = network_data.query_nodes(
                        f'SELECT COUNT(*) as count FROM nodes WHERE "group" = {thread_id % 10}'
                    )
                    count = result.column(0)[0].as_py()
                    results.append((thread_id, count))
            except Exception as e:
                errors.append((thread_id, str(e)))

        # Start multiple threads
        n_threads = 4
        threads = []

        start_time = time.time()

        for i in range(n_threads):
            thread = threading.Thread(target=query_worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        total_time = time.time() - start_time

        # Should complete without errors
        assert len(errors) == 0, f"Concurrent query errors: {errors}"
        assert len(results) == n_threads * 10  # Each thread makes 10 queries
        assert total_time < 30.0  # Should complete reasonably quickly

        print(
            f"Concurrent queries: {n_threads} threads, {len(results)} total queries in {total_time:.2f}s"
        )


class TestPerformanceRegression:
    """Test for performance regressions in key operations."""

    def test_networkx_import_performance(self):
        """Test performance of importing from NetworkX graphs."""
        # Create a moderately complex NetworkX graph
        graph = nx.barabasi_albert_graph(n=2000, m=3, seed=42)

        # Add attributes to make it more realistic
        for node in graph.nodes():
            graph.nodes[node]["group"] = node % 10
            graph.nodes[node]["weight"] = random.uniform(0, 100)

        for edge in graph.edges():
            graph.edges[edge]["weight"] = random.uniform(0, 10)
            graph.edges[edge]["type"] = random.choice(["A", "B", "C"])

        start_time = time.time()
        network_data = NetworkData.create_from_networkx_graph(graph)
        import_time = time.time() - start_time

        assert import_time < 20.0  # Should import reasonably quickly
        assert network_data.num_nodes == graph.number_of_nodes()
        assert network_data.num_edges == graph.number_of_edges()

        print(
            f"NetworkX import: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges in {import_time:.2f}s"
        )

    def test_table_augmentation_performance(self):
        """Test performance of table augmentation process."""
        n_nodes, n_edges = 5000, 15000

        # Create basic tables without augmentation
        nodes_data = {
            NODE_ID_COLUMN_NAME: list(range(n_nodes)),
            LABEL_COLUMN_NAME: [f"Node_{i}" for i in range(n_nodes)],
        }
        edges_data = {
            SOURCE_COLUMN_NAME: [
                random.randint(0, n_nodes - 1) for _ in range(n_edges)
            ],
            TARGET_COLUMN_NAME: [
                random.randint(0, n_nodes - 1) for _ in range(n_edges)
            ],
        }

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        # Test augmentation performance
        start_time = time.time()
        network_data = NetworkData.create_network_data(
            nodes_table, edges_table, augment_tables=True
        )
        augmentation_time = time.time() - start_time

        assert augmentation_time < 45.0  # Should augment reasonably quickly
        assert network_data.num_nodes == n_nodes
        assert network_data.num_edges == n_edges

        # Verify augmentation worked
        assert "_count_edges" in network_data.nodes.column_names
        assert "_edge_id" in network_data.edges.column_names

        print(
            f"Table augmentation: {n_nodes} nodes, {n_edges} edges in {augmentation_time:.2f}s"
        )
