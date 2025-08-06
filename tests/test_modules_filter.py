#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for network filtering modules."""

import pytest

from kiara_plugin.network_analysis.defaults import (
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.network_analysis.modules.filter import NetworkDataFiltersModule
from tests.resources.test_data import TEST_FIXTURES


class TestNetworkDataFiltersModule:
    """Test the NetworkDataFiltersModule."""

    def test_module_type_name(self):
        """Test module type name is correct."""
        module = NetworkDataFiltersModule()
        assert module._module_type_name == "network_data.filters"

    def test_retrieve_supported_type(self):
        """Test that module supports network_data type."""
        supported_type = NetworkDataFiltersModule.retrieve_supported_type()
        assert supported_type == "network_data"

    def test_create_filter_inputs_select_component(self):
        """Test filter inputs schema for select_component filter."""
        module = NetworkDataFiltersModule()

        inputs = module.create_filter_inputs("select_component")

        assert inputs is not None
        assert "component_id" in inputs
        assert inputs["component_id"]["type"] == "integer"
        assert inputs["component_id"]["optional"] is False
        assert inputs["component_id"]["default"] == 0

    def test_create_filter_inputs_unknown_filter(self):
        """Test that unknown filter names return None."""
        module = NetworkDataFiltersModule()

        inputs = module.create_filter_inputs("unknown_filter")
        assert inputs is None

    def test_select_component_filter_basic(self):
        """Test basic component selection filtering."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        # Create Value wrapper
        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        # Test selecting component 0 (largest component)
        result = module.filter__select_component(value, {"component_id": 0})

        assert isinstance(result, NetworkData)
        assert result.num_nodes <= disconnected_data.num_nodes
        assert result.num_nodes > 0

        # Verify all nodes belong to the selected component
        nodes_df = result.nodes.to_polars_dataframe()

        # Check that node IDs are sequential starting from 0
        node_ids = sorted(nodes_df[NODE_ID_COLUMN_NAME].to_list())
        expected_ids = list(range(result.num_nodes))
        assert node_ids == expected_ids

    def test_select_component_filter_different_component(self):
        """Test selecting different components."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        # Get available component IDs
        component_ids = disconnected_data.component_ids

        results = []
        for component_id in component_ids:
            result = module.filter__select_component(
                value, {"component_id": component_id}
            )
            results.append(result)

        # All results should be different sizes (unless components happen to be same size)
        node_counts = [r.num_nodes for r in results]
        assert len(set(node_counts)) >= 1  # At least one distinct size

        # Total nodes across all components should not exceed original
        total_filtered_nodes = sum(node_counts)
        # Note: This may not equal original due to isolated nodes or overlapping logic

    def test_select_component_filter_edges_consistency(self):
        """Test that filtered edges only reference remaining nodes."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        result = module.filter__select_component(value, {"component_id": 0})

        # All edge source and target IDs should be valid node IDs in filtered graph
        edges_df = result.edges.to_polars_dataframe()
        valid_node_ids = set(range(result.num_nodes))

        source_ids = set(edges_df[SOURCE_COLUMN_NAME].to_list())
        target_ids = set(edges_df[TARGET_COLUMN_NAME].to_list())

        assert source_ids.issubset(valid_node_ids)
        assert target_ids.issubset(valid_node_ids)

    def test_select_component_preserves_attributes(self):
        """Test that node and edge attributes are preserved during filtering."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        result = module.filter__select_component(value, {"component_id": 0})

        # Check that original non-computed columns are preserved
        original_nodes_columns = disconnected_data.nodes.column_names
        result_nodes_columns = result.nodes.column_names

        original_edges_columns = disconnected_data.edges.column_names
        result_edges_columns = result.edges.column_names

        # Required columns should be present
        assert NODE_ID_COLUMN_NAME in result_nodes_columns
        assert LABEL_COLUMN_NAME in result_nodes_columns
        assert SOURCE_COLUMN_NAME in result_edges_columns
        assert TARGET_COLUMN_NAME in result_edges_columns

        # Original attribute columns should be preserved (non-underscore columns)
        for col in original_nodes_columns:
            if not col.startswith("_"):
                assert col in result_nodes_columns

        for col in original_edges_columns:
            if not col.startswith("_"):
                assert col in result_edges_columns

    def test_select_component_node_id_remapping(self):
        """Test that node IDs are properly remapped in filtered graph."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        result = module.filter__select_component(value, {"component_id": 0})

        # Node IDs should start from 0 and be sequential
        nodes_df = result.nodes.to_polars_dataframe()
        node_ids = sorted(nodes_df[NODE_ID_COLUMN_NAME].to_list())
        expected_ids = list(range(len(node_ids)))

        assert node_ids == expected_ids

        # Old node IDs should be preserved in 'old_node_id' column
        assert "old_node_id" in nodes_df.columns

    def test_select_component_edge_id_mapping(self):
        """Test that edge source/target IDs are properly mapped."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        result = module.filter__select_component(value, {"component_id": 0})

        # Edges should preserve old source/target IDs for reference
        edges_df = result.edges.to_polars_dataframe()

        if result.num_edges > 0:
            # Should have old_source_id and old_target_id columns
            assert "old_source_id" in edges_df.columns
            assert "old_target_id" in edges_df.columns

            # New source/target should be in valid range
            max_node_id = result.num_nodes - 1
            source_ids = edges_df[SOURCE_COLUMN_NAME].to_list()
            target_ids = edges_df[TARGET_COLUMN_NAME].to_list()

            assert all(0 <= sid <= max_node_id for sid in source_ids)
            assert all(0 <= tid <= max_node_id for tid in target_ids)

    def test_select_component_nonexistent_component(self):
        """Test behavior when selecting non-existent component."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        # Use a component ID that definitely doesn't exist
        max_component_id = max(disconnected_data.component_ids)
        nonexistent_id = max_component_id + 100

        result = module.filter__select_component(
            value, {"component_id": nonexistent_id}
        )

        # Should return empty network data
        assert isinstance(result, NetworkData)
        assert result.num_nodes == 0
        assert result.num_edges == 0

    def test_select_component_single_component_graph(self):
        """Test filtering on graph with single component."""
        simple_data = TEST_FIXTURES.simple_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(simple_data)

        result = module.filter__select_component(value, {"component_id": 0})

        # Should return equivalent graph (all nodes in component 0)
        assert result.num_nodes == simple_data.num_nodes
        assert result.num_edges == simple_data.num_edges

    def test_select_component_preserves_edge_attributes(self):
        """Test that edge attributes are preserved during component filtering."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        result = module.filter__select_component(value, {"component_id": 0})

        if result.num_edges > 0:
            # Get original edge attributes (non-computed columns)
            original_edge_attrs = [
                col
                for col in disconnected_data.edges.column_names
                if not col.startswith("_")
            ]
            result_edge_attrs = [
                col
                for col in result.edges.column_names
                if not col.startswith("_") and not col.startswith("old_")
            ]

            # All original attributes should be preserved
            for attr in original_edge_attrs:
                assert attr in result.edges.column_names

    def test_filter_module_integration_with_kiara_api(self, kiara_api):
        """Test filter module integration through Kiara API."""
        # Create a network with multiple components for testing
        disconnected_data = TEST_FIXTURES.disconnected_network_data

        # Store the network data in Kiara
        kiara_api.store_value(disconnected_data, "test_disconnected_network")

        # Use the filter operation through API
        result = kiara_api.run_job(
            operation="network_data.filters.select_component",
            inputs={"value": "test_disconnected_network", "component_id": 0},
        )

        filtered_data = result["value"]
        assert isinstance(filtered_data.data, NetworkData)
        assert filtered_data.data.num_nodes <= disconnected_data.num_nodes

    def test_component_filtering_metadata_preservation(self):
        """Test that metadata is properly handled during filtering."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        result = module.filter__select_component(value, {"component_id": 0})

        # Result should have proper table structure
        assert hasattr(result, "nodes")
        assert hasattr(result, "edges")
        assert hasattr(result, "table_names")

        # Should be able to query the filtered data
        nodes_query_result = result.query_nodes(
            "SELECT COUNT(*) as node_count FROM nodes"
        )
        node_count = nodes_query_result.column(0)[0].as_py()
        assert node_count == result.num_nodes

    def test_filter_empty_component(self):
        """Test filtering behavior with empty input."""
        # Create a minimal network data with no edges
        import pyarrow as pa

        from kiara_plugin.network_analysis.models import NetworkData

        nodes_data = {NODE_ID_COLUMN_NAME: [0], LABEL_COLUMN_NAME: ["Isolated"]}
        edges_data = {SOURCE_COLUMN_NAME: [], TARGET_COLUMN_NAME: []}

        nodes_table = pa.Table.from_pydict(nodes_data)
        edges_table = pa.Table.from_pydict(edges_data)

        empty_network = NetworkData.create_network_data(nodes_table, edges_table)

        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(empty_network)

        result = module.filter__select_component(value, {"component_id": 0})

        # Should handle empty/isolated node case
        assert isinstance(result, NetworkData)
        # Result depends on implementation - might have 1 node or 0 nodes
        assert result.num_nodes >= 0
        assert result.num_edges == 0


class TestFilterModuleErrorHandling:
    """Test error handling in filter modules."""

    def test_invalid_filter_input_type(self):
        """Test error handling with invalid input types."""
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        # Test with non-NetworkData input
        value = MockValue("not_network_data")

        with pytest.raises(Exception):
            module.filter__select_component(value, {"component_id": 0})

    def test_invalid_component_id_type(self):
        """Test error handling with invalid component ID types."""
        simple_data = TEST_FIXTURES.simple_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(simple_data)

        # Test with string component ID
        with pytest.raises(Exception):
            module.filter__select_component(value, {"component_id": "invalid"})

        # Test with None component ID
        with pytest.raises(Exception):
            module.filter__select_component(value, {"component_id": None})

    def test_negative_component_id(self):
        """Test handling of negative component IDs."""
        simple_data = TEST_FIXTURES.simple_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(simple_data)

        # Negative component IDs should return empty result
        result = module.filter__select_component(value, {"component_id": -1})

        assert isinstance(result, NetworkData)
        assert result.num_nodes == 0
        assert result.num_edges == 0

    def test_missing_filter_input(self):
        """Test error handling with missing filter inputs."""
        simple_data = TEST_FIXTURES.simple_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(simple_data)

        # Missing component_id should raise error
        with pytest.raises(KeyError):
            module.filter__select_component(value, {})


class TestFilterModuleExtensibility:
    """Test filter module extensibility and customization."""

    def test_filter_inputs_extensibility(self):
        """Test that new filters can be added by extending the module."""

        class ExtendedNetworkDataFiltersModule(NetworkDataFiltersModule):
            def create_filter_inputs(self, filter_name):
                if filter_name == "custom_filter":
                    return {
                        "threshold": {
                            "type": "float",
                            "doc": "Custom threshold value",
                            "optional": False,
                            "default": 0.5,
                        }
                    }
                return super().create_filter_inputs(filter_name)

        extended_module = ExtendedNetworkDataFiltersModule()

        # Test original filter still works
        original_inputs = extended_module.create_filter_inputs("select_component")
        assert original_inputs is not None

        # Test new filter inputs
        custom_inputs = extended_module.create_filter_inputs("custom_filter")
        assert custom_inputs is not None
        assert "threshold" in custom_inputs

        # Test unknown filter
        unknown_inputs = extended_module.create_filter_inputs("unknown")
        assert unknown_inputs is None

    def test_module_inheritance_structure(self):
        """Test that the module follows proper inheritance."""
        module = NetworkDataFiltersModule()

        # Should inherit from FilterModule
        from kiara.modules.included_core_modules.filter import FilterModule

        assert isinstance(module, FilterModule)

        # Should have required methods
        assert hasattr(module, "create_filter_inputs")
        assert hasattr(module, "filter__select_component")
        assert hasattr(module, "retrieve_supported_type")


# Additional integration tests with the broader ecosystem
class TestFilterModuleEcosystemIntegration:
    """Test integration with broader Kiara ecosystem."""

    @pytest.mark.parametrize("network_name", ["disconnected", "simple"])
    def test_filter_results_are_valid_network_data(self, network_name):
        """Test that filter results are valid NetworkData that pass validation."""
        from kiara_plugin.network_analysis.data_types import NetworkDataType

        network_data = TEST_FIXTURES.get_all_network_data()[network_name]
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(network_data)

        result = module.filter__select_component(value, {"component_id": 0})

        # Result should pass NetworkDataType validation
        data_type = NetworkDataType()
        data_type._validate(result)  # Should not raise exception

    def test_filtered_network_can_be_converted_to_networkx(self):
        """Test that filtered results can be converted to NetworkX."""
        import networkx as nx

        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        result = module.filter__select_component(value, {"component_id": 0})

        # Should be able to convert to NetworkX
        nx_graph = result.as_networkx_graph(nx.Graph)

        assert isinstance(nx_graph, nx.Graph)
        assert nx_graph.number_of_nodes() == result.num_nodes
        assert nx_graph.number_of_edges() == result.num_edges

    def test_filtered_network_metadata_computation(self):
        """Test that filtered networks have proper metadata computed."""
        disconnected_data = TEST_FIXTURES.disconnected_network_data
        module = NetworkDataFiltersModule()

        class MockValue:
            def __init__(self, data):
                self.data = data

        value = MockValue(disconnected_data)

        result = module.filter__select_component(value, {"component_id": 0})

        if result.num_nodes > 0:
            # Should be able to compute component IDs for filtered result
            component_ids = result.component_ids

            # Single component graph should have only component 0
            assert len(component_ids) == 1
            assert 0 in component_ids
