#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for network creation modules."""

import os
import tempfile

import pytest

from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import KiaraFile
from kiara_plugin.network_analysis.defaults import (
    LABEL_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.network_analysis.modules.create import (
    AssembleGraphFromTablesModule,
    CreateNetworkDataModule,
)
from tests.resources.test_data import TEST_FIXTURES, create_test_file_content


class TestCreateNetworkDataModule:
    """Test the CreateNetworkDataModule for file imports."""

    @pytest.fixture
    def temp_files(self):
        """Create temporary test files."""
        file_contents = create_test_file_content()
        temp_files = {}

        for filename, content in file_contents.items():
            if filename.endswith(".csv"):
                continue  # Skip CSV files for this module

            temp_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=f".{filename.split('.')[-1]}", delete=False
            )
            temp_file.write(content)
            temp_file.flush()
            temp_files[filename] = temp_file.name

        yield temp_files

        # Cleanup
        for filepath in temp_files.values():
            try:
                os.unlink(filepath)
            except OSError:
                pass

    def test_create_from_gml_file(self, temp_files):
        """Test creating NetworkData from GML file."""
        module = CreateNetworkDataModule()
        gml_file = temp_files["simple.gml"]

        # Create KiaraFile object
        kiara_file = KiaraFile.create_from_file_path(gml_file)

        # Create Value object
        class MockValue:
            def __init__(self, data):
                self.data = data

        source_value = MockValue(kiara_file)

        result = module.create__network_data__from__file(source_value)

        assert isinstance(result, NetworkData)
        assert result.num_nodes == 3
        assert result.num_edges == 3

        # Check that node labels are preserved
        nodes_df = result.nodes.to_polars_dataframe()
        labels = set(nodes_df[LABEL_COLUMN_NAME].to_list())
        assert "A" in labels
        assert "B" in labels
        assert "C" in labels

    def test_create_from_gexf_file(self, temp_files):
        """Test creating NetworkData from GEXF file."""
        module = CreateNetworkDataModule()
        gexf_file = temp_files["simple.gexf"]

        kiara_file = KiaraFile.create_from_file_path(gexf_file)

        class MockValue:
            def __init__(self, data):
                self.data = data

        source_value = MockValue(kiara_file)

        result = module.create__network_data__from__file(source_value)

        assert isinstance(result, NetworkData)
        assert result.num_nodes == 3
        assert result.num_edges == 3

    def test_create_from_networkx_graph_direct(self):
        """Test creating NetworkData from NetworkX graph through the module."""
        # This tests the underlying NetworkX conversion functionality
        graph = TEST_FIXTURES.simple_graph

        # The module doesn't directly expose NetworkX conversion, but we can test
        # the underlying functionality it uses
        result = NetworkData.create_from_networkx_graph(graph)

        assert isinstance(result, NetworkData)
        assert result.num_nodes == graph.number_of_nodes()
        assert result.num_edges == graph.number_of_edges()

    def test_unsupported_file_format(self):
        """Test error handling for unsupported file formats."""
        module = CreateNetworkDataModule()

        # Create temporary file with unsupported extension
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
        temp_file.write("unsupported content")
        temp_file.flush()

        try:
            kiara_file = KiaraFile.create_from_file_path(temp_file.name)

            class MockValue:
                def __init__(self, data):
                    self.data = data

            source_value = MockValue(kiara_file)

            with pytest.raises(KiaraProcessingException, match="unsupported format"):
                module.create__network_data__from__file(source_value)
        finally:
            os.unlink(temp_file.name)

    def test_supported_file_extensions(self):
        """Test that all documented file extensions are handled."""
        module = CreateNetworkDataModule()

        # Get the docstring to check documented formats
        doc = module.create__network_data__from__file.__doc__
        assert "gml" in doc
        assert "gexf" in doc
        assert "graphml" in doc
        assert "pajek" in doc
        assert "leda" in doc
        assert "graph6" in doc
        assert "sparse6" in doc

    def test_module_config(self):
        """Test module configuration."""
        module = CreateNetworkDataModule()

        assert hasattr(module, "_config_cls")
        assert module._module_type_name == "create.network_data"


class TestAssembleGraphFromTablesModule:
    """Test the AssembleGraphFromTablesModule."""

    @pytest.fixture
    def sample_tables(self, kiara_api):
        """Create sample tables for testing."""
        # Create nodes CSV data
        nodes_data = """id,label,type,value
0,Alice,person,1.0
1,Bob,person,2.0
2,Charlie,organization,3.0"""

        # Create edges CSV data
        edges_data = """source,target,weight,relationship
0,1,1.5,friend
1,2,2.5,colleague
0,2,0.5,acquaintance"""

        # Create temporary files
        nodes_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        nodes_file.write(nodes_data)
        nodes_file.flush()

        edges_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        edges_file.write(edges_data)
        edges_file.flush()

        # Import as Kiara tables
        nodes_table = kiara_api.run_job(
            operation="create.table.from.file", inputs={"file": nodes_file.name}
        )["table"]

        edges_table = kiara_api.run_job(
            operation="create.table.from.file", inputs={"file": edges_file.name}
        )["table"]

        yield nodes_table, edges_table

        # Cleanup
        os.unlink(nodes_file.name)
        os.unlink(edges_file.name)

    def test_assemble_with_both_tables(self, kiara_api, sample_tables):
        """Test assembling network data with both nodes and edges tables."""
        nodes_table, edges_table = sample_tables

        result = kiara_api.run_job(
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

        network_data = result["network_data"]
        assert isinstance(network_data.data, NetworkData)
        assert network_data.data.num_nodes == 3
        assert network_data.data.num_edges == 3

    def test_assemble_edges_only(self, kiara_api, sample_tables):
        """Test assembling network data with edges table only."""
        _, edges_table = sample_tables

        result = kiara_api.run_job(
            operation="assemble.network_data",
            inputs={
                "edges": edges_table,
                "source_column": "source",
                "target_column": "target",
            },
        )

        network_data = result["network_data"]
        assert isinstance(network_data.data, NetworkData)
        assert network_data.data.num_edges == 3

        # Nodes should be auto-generated from edge endpoints
        assert network_data.data.num_nodes >= 2  # At least source and target nodes

    def test_column_auto_detection(self, kiara_api, sample_tables):
        """Test automatic column detection."""
        nodes_table, edges_table = sample_tables

        # Don't specify column names, let module auto-detect
        result = kiara_api.run_job(
            operation="assemble.network_data",
            inputs={"edges": edges_table, "nodes": nodes_table},
        )

        network_data = result["network_data"]
        assert isinstance(network_data.data, NetworkData)
        assert network_data.data.num_nodes == 3
        assert network_data.data.num_edges == 3

    def test_column_mapping(self, kiara_api, sample_tables):
        """Test column mapping functionality."""
        nodes_table, edges_table = sample_tables

        result = kiara_api.run_job(
            operation="assemble.network_data",
            inputs={
                "edges": edges_table,
                "nodes": nodes_table,
                "source_column": "source",
                "target_column": "target",
                "id_column": "id",
                "label_column": "label",
                "edges_column_map": {"weight": "edge_weight"},
                "nodes_column_map": {"type": "node_type"},
            },
        )

        network_data = result["network_data"]

        # Check that columns were mapped correctly
        edges_columns = network_data.data.edges.column_names
        nodes_columns = network_data.data.nodes.column_names

        assert "edge_weight" in edges_columns
        assert "node_type" in nodes_columns

    def test_module_inputs_schema(self):
        """Test module input schema definition."""
        module = AssembleGraphFromTablesModule()

        inputs_schema = module.create_inputs_schema()

        # Check required inputs
        assert "edges" in inputs_schema
        assert inputs_schema["edges"]["type"] == "table"
        assert inputs_schema["edges"]["optional"] is False

        # Check optional inputs
        assert "nodes" in inputs_schema
        assert inputs_schema["nodes"]["optional"] is True

        assert "source_column" in inputs_schema
        assert inputs_schema["source_column"]["optional"] is True

    def test_module_outputs_schema(self):
        """Test module output schema definition."""
        module = AssembleGraphFromTablesModule()

        outputs_schema = module.create_outputs_schema()

        assert "network_data" in outputs_schema
        assert outputs_schema["network_data"]["type"] == "network_data"

    def test_invalid_column_specification(self, kiara_api, sample_tables):
        """Test error handling for invalid column specifications."""
        nodes_table, edges_table = sample_tables

        with pytest.raises((KiaraProcessingException, Exception)):
            kiara_api.run_job(
                operation="assemble.network_data",
                inputs={
                    "edges": edges_table,
                    "nodes": nodes_table,
                    "source_column": "nonexistent_source",
                    "target_column": "target",
                },
            )

    def test_missing_node_references(self, kiara_api):
        """Test error handling when edges reference non-existent nodes."""
        # Create edges that reference nodes not in nodes table
        edges_data = """source,target
0,1
1,5"""  # Node 5 doesn't exist in nodes

        nodes_data = """id,label
0,A
1,B"""  # Only nodes 0 and 1

        nodes_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        nodes_file.write(nodes_data)
        nodes_file.flush()

        edges_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        edges_file.write(edges_data)
        edges_file.flush()

        try:
            nodes_table = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": nodes_file.name}
            )["table"]

            edges_table = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": edges_file.name}
            )["table"]

            # This should succeed because missing nodes are automatically added
            result = kiara_api.run_job(
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

            network_data = result["network_data"]
            # Should have 3 nodes (0, 1, and auto-added 5)
            assert network_data.data.num_nodes == 3

        finally:
            os.unlink(nodes_file.name)
            os.unlink(edges_file.name)

    def test_edge_column_auto_detection_fallback(self, kiara_api):
        """Test fallback behavior when auto-detection fails."""
        # Create edges table with only two columns (no headers that match defaults)
        edges_data = """col1,col2
0,1
1,2"""

        edges_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        edges_file.write(edges_data)
        edges_file.flush()

        try:
            edges_table = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": edges_file.name}
            )["table"]

            # Should use first two columns as source and target
            result = kiara_api.run_job(
                operation="assemble.network_data", inputs={"edges": edges_table}
            )

            network_data = result["network_data"]
            assert network_data.data.num_edges == 2

        finally:
            os.unlink(edges_file.name)

    def test_duplicate_node_ids_handling(self, kiara_api):
        """Test handling of duplicate node IDs."""
        nodes_data = """id,label
0,A
1,B
0,A_duplicate"""  # Duplicate ID

        edges_data = """source,target
0,1"""

        nodes_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        nodes_file.write(nodes_data)
        nodes_file.flush()

        edges_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        edges_file.write(edges_data)
        edges_file.flush()

        try:
            nodes_table = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": nodes_file.name}
            )["table"]

            edges_table = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": edges_file.name}
            )["table"]

            # Should handle duplicate IDs (behavior depends on implementation)
            result = kiara_api.run_job(
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

            network_data = result["network_data"]
            # Should successfully create network data
            assert network_data.data.num_edges == 1

        finally:
            os.unlink(nodes_file.name)
            os.unlink(edges_file.name)


class TestCreateModulesIntegration:
    """Test integration between different creation modules."""

    def test_create_and_assemble_compatibility(self, kiara_api):
        """Test that outputs from create modules work with assemble modules."""
        # This is more of an integration test showing how the modules work together

        # Create a simple table first
        table_data = """source,target,weight
0,1,1.0
1,2,2.0
2,0,3.0"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        temp_file.write(table_data)
        temp_file.flush()

        try:
            # Create table from file
            table_result = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": temp_file.name}
            )

            # Use table to create network data
            network_result = kiara_api.run_job(
                operation="assemble.network_data",
                inputs={
                    "edges": table_result["table"],
                    "source_column": "source",
                    "target_column": "target",
                },
            )

            network_data = network_result["network_data"]
            assert isinstance(network_data.data, NetworkData)
            assert network_data.data.num_edges == 3

        finally:
            os.unlink(temp_file.name)

    def test_module_metadata(self):
        """Test module metadata and documentation."""
        create_module = CreateNetworkDataModule()
        assemble_module = AssembleGraphFromTablesModule()

        # Check module type names
        assert create_module._module_type_name == "create.network_data"
        assert assemble_module._module_type_name == "assemble.network_data"

        # Check that modules have proper documentation
        assert create_module.__doc__ is not None
        assert assemble_module.__doc__ is not None

        # Check config classes
        assert hasattr(create_module, "_config_cls")
        assert hasattr(assemble_module, "_config_cls")


class TestModuleErrorHandling:
    """Test error handling across creation modules."""

    def test_malformed_file_handling(self):
        """Test handling of malformed input files."""
        module = CreateNetworkDataModule()

        # Create malformed GML file
        malformed_gml = """graph [
  node [
    id 0
    # Missing closing bracket
  edge [
    source 0
    target 1
  ]
]"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write(malformed_gml)
        temp_file.flush()

        try:
            kiara_file = KiaraFile.create_from_file_path(temp_file.name)

            class MockValue:
                def __init__(self, data):
                    self.data = data

            source_value = MockValue(kiara_file)

            # Should raise an exception for malformed file
            with pytest.raises(Exception):  # NetworkX will raise parsing exception
                module.create__network_data__from__file(source_value)

        finally:
            os.unlink(temp_file.name)

    def test_empty_file_handling(self):
        """Test handling of empty input files."""
        module = CreateNetworkDataModule()

        # Create empty GML file
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write("")
        temp_file.flush()

        try:
            kiara_file = KiaraFile.create_from_file_path(temp_file.name)

            class MockValue:
                def __init__(self, data):
                    self.data = data

            source_value = MockValue(kiara_file)

            # Should handle empty file gracefully (may create empty graph)
            with pytest.raises(Exception):  # NetworkX will likely raise an exception
                module.create__network_data__from__file(source_value)

        finally:
            os.unlink(temp_file.name)

    def test_non_existent_file_columns(self, kiara_api):
        """Test error handling for column specifications that don't exist."""
        edges_data = """a,b
0,1
1,2"""

        edges_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        edges_file.write(edges_data)
        edges_file.flush()

        try:
            edges_table = kiara_api.run_job(
                operation="create.table.from.file", inputs={"file": edges_file.name}
            )["table"]

            # Specify columns that don't exist
            with pytest.raises((KiaraProcessingException, Exception)):
                kiara_api.run_job(
                    operation="assemble.network_data",
                    inputs={
                        "edges": edges_table,
                        "source_column": "nonexistent_source",
                        "target_column": "nonexistent_target",
                    },
                )

        finally:
            os.unlink(edges_file.name)
