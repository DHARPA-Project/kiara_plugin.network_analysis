#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Integration tests for network analysis plugin workflows."""

import os
import tempfile
from pathlib import Path

import networkx as nx
import pytest

from kiara_plugin.network_analysis.models import NetworkData
from tests.resources.test_data import TEST_FIXTURES


class TestEndToEndWorkflows:
    """Test complete end-to-end workflows."""

    def test_csv_to_network_to_analysis_workflow(self, kiara_api):
        """Test complete workflow from CSV files to network analysis."""
        # Create test CSV files
        nodes_csv_content = """id,label,type,importance
0,Alice,person,0.8
1,Bob,person,0.6
2,Charlie,person,0.7
3,DataCorp,organization,0.9"""

        edges_csv_content = """source,target,weight,relationship
0,1,1.5,colleague
1,2,2.0,friend
0,3,0.8,works_for
2,3,1.2,consultant"""

        # Create temporary files
        nodes_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        nodes_file.write(nodes_csv_content)
        nodes_file.flush()

        edges_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        edges_file.write(edges_csv_content)
        edges_file.flush()

        try:
            # Step 1: Import CSV files as tables
            nodes_table_result = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": nodes_file.name}
            )
            nodes_table = nodes_table_result["table"]

            edges_table_result = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": edges_file.name}
            )
            edges_table = edges_table_result["table"]

            # Step 2: Create network data from tables
            network_result = kiara_api.run_job(
                operation="assemble.network_data",
                inputs={
                    "edges": edges_table,
                    "nodes": nodes_table,
                    "source_column": "source",
                    "target_column": "target",
                    "id_column": "id",
                    "label_column": "label",
                },
            )
            network_data = network_result["network_data"]

            # Verify network structure
            assert isinstance(network_data.data, NetworkData)
            assert network_data.data.num_nodes == 4
            assert network_data.data.num_edges == 4

            # Step 3: Analyze network properties
            component_ids = network_data.data.component_ids
            assert len(component_ids) == 1  # Should be connected

            # Step 4: Convert to NetworkX for further analysis
            nx_graph = network_data.data.as_networkx_graph(
                nx.Graph, incl_node_attributes=True, incl_edge_attributes=True
            )

            # Verify NetworkX conversion preserved data
            assert nx_graph.number_of_nodes() == 4
            assert nx_graph.number_of_edges() == 4

            # Check that attributes were preserved
            node_data = nx_graph.nodes[0]  # First node
            assert "type" in node_data
            assert "importance" in node_data

            edge_data = list(nx_graph.edges(data=True))[0][2]  # First edge
            assert "weight" in edge_data
            assert "relationship" in edge_data

        finally:
            os.unlink(nodes_file.name)
            os.unlink(edges_file.name)

    def test_gml_import_to_filtered_export_workflow(self, kiara_api):
        """Test workflow from GML import to filtered network export."""
        # Create a GML file with multiple components
        gml_content = """graph [
  node [ id 0 label "A" component "first" ]
  node [ id 1 label "B" component "first" ]
  node [ id 2 label "C" component "first" ]
  node [ id 3 label "X" component "second" ]
  node [ id 4 label "Y" component "second" ]
  node [ id 5 label "Z" component "isolated" ]

  edge [ source 0 target 1 weight 1.0 ]
  edge [ source 1 target 2 weight 2.0 ]
  edge [ source 2 target 0 weight 1.5 ]
  edge [ source 3 target 4 weight 3.0 ]
]"""

        gml_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        gml_file.write(gml_content)
        gml_file.flush()

        try:
            # Step 1: Import GML file
            import_result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": gml_file.name},
            )
            full_network = import_result["network_data"]

            # Verify import
            assert full_network.data.num_nodes == 6
            assert full_network.data.num_edges == 4

            # Step 2: Filter to select largest component
            filter_result = kiara_api.run_job(
                operation="network_data.filters.select_component",
                inputs={
                    "value": full_network,
                    "component_id": 0,  # Largest component
                },
            )
            filtered_network = filter_result["value"]

            # Verify filtering worked
            assert filtered_network.data.num_nodes <= full_network.data.num_nodes
            assert filtered_network.data.num_nodes >= 3  # Should have the triangle

            # Step 3: Convert filtered network to NetworkX
            nx_graph = filtered_network.data.as_networkx_graph(
                nx.Graph, incl_node_attributes=True
            )

            # Verify final result
            assert nx.is_connected(nx_graph)  # Filtered component should be connected

        finally:
            os.unlink(gml_file.name)

    def test_networkx_roundtrip_workflow(self):
        """Test roundtrip conversion: NetworkX -> NetworkData -> NetworkX."""
        # Start with a complex NetworkX graph
        original_graph = nx.karate_club_graph()

        # Add some attributes
        for node in original_graph.nodes():
            original_graph.nodes[node]["group"] = node % 3
            original_graph.nodes[node]["centrality"] = nx.degree_centrality(
                original_graph
            )[node]

        for edge in original_graph.edges():
            original_graph.edges[edge]["weight"] = 1.0 / (abs(edge[0] - edge[1]) + 1)

        # Convert to NetworkData
        network_data = NetworkData.create_from_networkx_graph(original_graph)

        # Verify conversion
        assert network_data.num_nodes == original_graph.number_of_nodes()
        assert network_data.num_edges == original_graph.number_of_edges()

        # Convert back to NetworkX
        final_graph = network_data.as_networkx_graph(
            type(original_graph), incl_node_attributes=True, incl_edge_attributes=True
        )

        # Verify roundtrip preservation
        assert final_graph.number_of_nodes() == original_graph.number_of_nodes()
        assert final_graph.number_of_edges() == original_graph.number_of_edges()

        # Check structure is preserved
        assert set(original_graph.nodes()) == set(final_graph.nodes())
        assert set(original_graph.edges()) == set(final_graph.edges())

        # Check node attributes are preserved (approximately)
        for node in original_graph.nodes():
            original_attrs = original_graph.nodes[node]
            final_attrs = final_graph.nodes[node]
            assert "group" in final_attrs
            # Centrality might be slightly different due to data type conversions


class TestModuleChaining:
    """Test chaining multiple modules together."""

    def test_create_assemble_filter_chain(self, kiara_api):
        """Test chaining create -> assemble -> filter operations."""
        # Create test data files
        edges_csv = """source,target,weight,component
0,1,1.0,A
1,2,2.0,A
3,4,1.5,B
4,5,2.5,B
6,6,1.0,C"""  # Self-loop isolated node

        edges_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        edges_file.write(edges_csv)
        edges_file.flush()

        try:
            # Step 1: Create table
            table_result = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": edges_file.name}
            )
            edges_table = table_result["table"]

            # Step 2: Assemble network (auto-generate nodes)
            network_result = kiara_api.run_job(
                operation="assemble.network_data",
                inputs={
                    "edges": edges_table,
                    "source_column": "source",
                    "target_column": "target",
                },
            )
            full_network = network_result["network_data"]

            # Step 3: Filter to largest component
            filter_result = kiara_api.run_job(
                operation="network_data.filters.select_component",
                inputs={"value": full_network, "component_id": 0},
            )
            filtered_network = filter_result["value"]

            # Verify chain worked
            assert full_network.data.num_edges == 5
            assert filtered_network.data.num_nodes <= full_network.data.num_nodes
            assert filtered_network.data.num_edges <= full_network.data.num_edges

        finally:
            os.unlink(edges_file.name)

    def test_multiple_filter_operations(self, kiara_api):
        """Test applying multiple filters in sequence."""
        # Use a test graph with known components
        network_data = TEST_FIXTURES.disconnected_network_data

        # Store in kiara for use in operations
        kiara_api.store_value(network_data, "test_network")

        # Apply first filter
        filter1_result = kiara_api.run_job(
            operation="network_data.filters.select_component",
            inputs={"value": "test_network", "component_id": 0},
        )
        component0_network = filter1_result["value"]

        # Store intermediate result
        kiara_api.store_value(component0_network, "component0_network")

        # Could apply additional filters here if they existed
        # For now, just verify we can continue the chain

        # Convert to NetworkX as final step
        nx_graph = component0_network.data.as_networkx_graph(nx.Graph)

        # Verify chain results
        assert component0_network.data.num_nodes <= network_data.num_nodes
        assert nx_graph.number_of_nodes() == component0_network.data.num_nodes


class TestDataFlowIntegration:
    """Test data flow between different parts of the system."""

    def test_metadata_propagation(self, kiara_api):
        """Test that metadata is properly propagated through operations."""
        # Create network with rich metadata
        nodes_csv = """id,label,type,department,salary
0,Alice,manager,Engineering,75000
1,Bob,developer,Engineering,65000
2,Charlie,developer,Sales,60000
3,DataCorp,company,Corporate,0"""

        edges_csv = """source,target,weight,relationship,years
0,1,0.8,supervises,2.5
1,2,0.6,collaborates,1.0
0,3,0.9,works_for,5.0"""

        nodes_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        nodes_file.write(nodes_csv)
        nodes_file.flush()

        edges_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        edges_file.write(edges_csv)
        edges_file.flush()

        try:
            # Import tables
            nodes_result = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": nodes_file.name}
            )
            edges_result = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": edges_file.name}
            )

            # Create network
            network_result = kiara_api.run_job(
                operation="assemble.network_data",
                inputs={
                    "edges": edges_result["table"],
                    "nodes": nodes_result["table"],
                    "source_column": "source",
                    "target_column": "target",
                    "id_column": "id",
                    "label_column": "label",
                },
            )

            network_data = network_result["network_data"].data

            # Verify metadata preservation
            nodes_df = network_data.nodes.to_polars_dataframe()
            edges_df = network_data.edges.to_polars_dataframe()

            # Check node metadata
            expected_node_cols = ["type", "department", "salary"]
            for col in expected_node_cols:
                assert col in nodes_df.columns

            # Check edge metadata
            expected_edge_cols = ["relationship", "years"]
            for col in expected_edge_cols:
                assert col in edges_df.columns

            # Verify computed metadata is also present
            assert "_count_edges" in nodes_df.columns
            assert "_edge_id" in edges_df.columns

        finally:
            os.unlink(nodes_file.name)
            os.unlink(edges_file.name)

    def test_value_storage_and_retrieval(self, kiara_api):
        """Test storing and retrieving NetworkData values."""
        # Create a network
        network_data = TEST_FIXTURES.simple_network_data

        # Store with alias
        kiara_api.store_value(network_data, "stored_network")

        # Retrieve and verify
        retrieved_value = kiara_api.get_value("stored_network")
        retrieved_network = retrieved_value.data

        assert isinstance(retrieved_network, NetworkData)
        assert retrieved_network.num_nodes == network_data.num_nodes
        assert retrieved_network.num_edges == network_data.num_edges

        # Use stored value in operation
        filter_result = kiara_api.run_job(
            operation="network_data.filters.select_component",
            inputs={"value": "stored_network", "component_id": 0},
        )

        filtered_network = filter_result["value"]
        assert isinstance(filtered_network.data, NetworkData)


class TestErrorRecoveryIntegration:
    """Test error handling and recovery in integrated workflows."""

    def test_invalid_file_recovery(self, kiara_api):
        """Test recovery from invalid file imports."""
        # Create an invalid file
        invalid_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".gml", delete=False
        )
        invalid_file.write("not valid gml content at all")
        invalid_file.flush()

        try:
            # This should fail
            with pytest.raises(Exception):
                kiara_api.run_job(
                    operation="create.network_data.from.file",
                    inputs={"file": invalid_file.name},
                )

            # But we should be able to continue with valid operations
            valid_network = TEST_FIXTURES.simple_network_data
            kiara_api.store_value(valid_network, "recovery_network")

            # This should work
            result = kiara_api.run_job(
                operation="network_data.filters.select_component",
                inputs={"value": "recovery_network", "component_id": 0},
            )

            assert isinstance(result["value"].data, NetworkData)

        finally:
            os.unlink(invalid_file.name)

    def test_invalid_operation_parameters(self, kiara_api):
        """Test handling of invalid operation parameters."""
        network_data = TEST_FIXTURES.simple_network_data
        kiara_api.store_value(network_data, "param_test_network")

        # Test invalid component ID
        with pytest.raises(Exception):
            kiara_api.run_job(
                operation="network_data.filters.select_component",
                inputs={
                    "value": "param_test_network",
                    "component_id": -999,  # Invalid component ID
                },
            )

        # But valid parameters should still work
        valid_result = kiara_api.run_job(
            operation="network_data.filters.select_component",
            inputs={
                "value": "param_test_network",
                "component_id": 0,  # Valid component ID
            },
        )

        assert isinstance(valid_result["value"].data, NetworkData)


class TestPerformanceIntegration:
    """Test performance of integrated workflows."""

    def test_large_workflow_performance(self, kiara_api):
        """Test performance of workflow with larger data."""
        # Create larger test files
        n_nodes = 1000
        n_edges = 2000

        # Generate nodes CSV
        nodes_lines = ["id,label,group"]
        nodes_lines.extend([f"{i},Node_{i},{i % 10}" for i in range(n_nodes)])
        nodes_content = "\n".join(nodes_lines)

        # Generate edges CSV
        import random

        random.seed(42)
        edges_lines = ["source,target,weight"]
        edges_lines.extend(
            [
                f"{random.randint(0, n_nodes - 1)},{random.randint(0, n_nodes - 1)},{random.random():.3f}"
                for _ in range(n_edges)
            ]
        )
        edges_content = "\n".join(edges_lines)

        nodes_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        nodes_file.write(nodes_content)
        nodes_file.flush()

        edges_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        edges_file.write(edges_content)
        edges_file.flush()

        try:
            import time

            start_time = time.time()

            # Full workflow
            nodes_result = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": nodes_file.name}
            )

            edges_result = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": edges_file.name}
            )

            network_result = kiara_api.run_job(
                operation="assemble.network_data",
                inputs={
                    "edges": edges_result["table"],
                    "nodes": nodes_result["table"],
                    "source_column": "source",
                    "target_column": "target",
                    "id_column": "id",
                    "label_column": "label",
                },
            )

            filter_result = kiara_api.run_job(
                operation="network_data.filters.select_component",
                inputs={"value": network_result["network_data"], "component_id": 0},
            )

            total_time = time.time() - start_time

            # Should complete in reasonable time
            assert total_time < 60.0  # 1 minute max

            # Verify results
            final_network = filter_result["value"].data
            assert isinstance(final_network, NetworkData)
            assert final_network.num_nodes <= n_nodes

            print(
                f"Large workflow ({n_nodes} nodes, {n_edges} edges): {total_time:.2f}s"
            )

        finally:
            os.unlink(nodes_file.name)
            os.unlink(edges_file.name)


class TestCompatibilityIntegration:
    """Test compatibility with other kiara plugins and systems."""

    def test_tabular_plugin_integration(self, kiara_api):
        """Test integration with tabular plugin operations."""
        # Create network data
        network_data = TEST_FIXTURES.simple_network_data

        # Extract nodes table for use with tabular operations
        nodes_table = network_data.nodes

        # Should be compatible with tabular operations
        # (This tests that our NetworkData tables work with tabular plugin)

        # Test querying the table directly
        result = kiara_api.run_job(
            operation="query.table",
            inputs={
                "table": nodes_table,
                "query": "SELECT COUNT(*) as node_count FROM table_alias",
            },
        )

        query_result = result["query_result"]
        assert query_result is not None

        # Test table export
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        temp_file.close()  # Close so kiara can write to it

        try:
            export_result = kiara_api.run_job(
                operation="export.table.as.file",
                inputs={"table": nodes_table, "file_path": temp_file.name},
            )

            # Should export successfully
            assert Path(temp_file.name).exists()

            # File should have content
            with open(temp_file.name, "r") as f:
                content = f.read()
                assert len(content) > 0
                assert "_node_id" in content  # Should have our node ID column

        finally:
            try:
                os.unlink(temp_file.name)
            except OSError:
                pass

    @pytest.mark.parametrize("graph_type", [nx.Graph, nx.DiGraph])
    def test_networkx_compatibility(self, graph_type):
        """Test compatibility with different NetworkX graph types."""
        # Create NetworkData from test fixture
        if graph_type == nx.Graph:
            original_graph = TEST_FIXTURES.simple_graph
        else:  # DiGraph
            original_graph = TEST_FIXTURES.simple_digraph

        network_data = NetworkData.create_from_networkx_graph(original_graph)

        # Convert back to specified type
        converted_graph = network_data.as_networkx_graph(
            graph_type, incl_node_attributes=True, incl_edge_attributes=True
        )

        # Should be correct type
        assert isinstance(converted_graph, graph_type)

        # Should preserve basic structure
        assert converted_graph.number_of_nodes() == original_graph.number_of_nodes()

        # Should be compatible with NetworkX algorithms
        if graph_type == nx.Graph:
            # Test undirected graph algorithms
            assert nx.is_connected(converted_graph) or not nx.is_connected(
                original_graph
            )
            centrality = nx.degree_centrality(converted_graph)
            assert len(centrality) == converted_graph.number_of_nodes()
        else:
            # Test directed graph algorithms
            assert nx.is_weakly_connected(
                converted_graph
            ) or not nx.is_weakly_connected(original_graph)
            centrality = nx.in_degree_centrality(converted_graph)
            assert len(centrality) == converted_graph.number_of_nodes()
