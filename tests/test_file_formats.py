#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for multi-format network file import and export."""

import os
import tempfile

import networkx as nx
import pytest

from kiara_plugin.network_analysis.models import NetworkData
from tests.resources.test_data import TEST_FIXTURES, create_test_file_content


class TestNetworkFileFormats:
    """Test support for various network file formats."""

    @pytest.fixture
    def test_files(self):
        """Create temporary test files in various formats."""
        file_contents = create_test_file_content()
        temp_files = {}

        for filename, content in file_contents.items():
            if filename.endswith((".csv", ".nodes", ".edges")):
                continue  # Skip CSV files in this test

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

    def test_gml_format_support(self, test_files, kiara_api):
        """Test GML file format support."""
        gml_file = test_files["simple.gml"]

        result = kiara_api.run_job(
            operation="create.network_data.from.file", inputs={"file": gml_file}
        )

        network_data = result["network_data"]
        assert isinstance(network_data.data, NetworkData)
        assert network_data.data.num_nodes == 3
        assert network_data.data.num_edges == 3

        # Verify node attributes are preserved
        nodes_df = network_data.data.nodes.to_polars_dataframe()
        assert "value" in nodes_df.columns

        # Verify edge attributes are preserved
        edges_df = network_data.data.edges.to_polars_dataframe()
        assert "weight" in edges_df.columns

    def test_gexf_format_support(self, test_files, kiara_api):
        """Test GEXF file format support."""
        gexf_file = test_files["simple.gexf"]

        result = kiara_api.run_job(
            operation="create.network_data.from.file", inputs={"file": gexf_file}
        )

        network_data = result["network_data"]
        assert isinstance(network_data.data, NetworkData)
        assert network_data.data.num_nodes == 3
        assert network_data.data.num_edges == 3

    def test_unsupported_format_error(self, kiara_api):
        """Test error handling for unsupported file formats."""
        # Create a file with unsupported extension
        temp_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".unsupported", delete=False
        )
        temp_file.write("unsupported content")
        temp_file.flush()

        try:
            with pytest.raises(Exception):  # Should raise processing exception
                kiara_api.run_job(
                    operation="create.network_data.from.file",
                    inputs={"file": temp_file.name},
                )
        finally:
            os.unlink(temp_file.name)

    def test_empty_file_handling(self, kiara_api):
        """Test handling of empty network files."""
        # Create empty GML file
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write("")
        temp_file.flush()

        try:
            with pytest.raises(Exception):  # NetworkX should raise parsing error
                kiara_api.run_job(
                    operation="create.network_data.from.file",
                    inputs={"file": temp_file.name},
                )
        finally:
            os.unlink(temp_file.name)

    def test_malformed_gml_file(self, kiara_api):
        """Test handling of malformed GML files."""
        malformed_gml = """graph [
  node [
    id 0
    # Missing closing bracket for node
  edge [
    source 0
    target 1
  ]
]"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write(malformed_gml)
        temp_file.flush()

        try:
            with pytest.raises(Exception):  # Should raise parsing error
                kiara_api.run_job(
                    operation="create.network_data.from.file",
                    inputs={"file": temp_file.name},
                )
        finally:
            os.unlink(temp_file.name)


class TestNetworkFileFormatCompatibility:
    """Test compatibility across different network file formats."""

    @pytest.fixture
    def networkx_graphs(self):
        """Create NetworkX graphs for testing format compatibility."""
        return {
            "simple": TEST_FIXTURES.simple_graph,
            "digraph": TEST_FIXTURES.simple_digraph,
            "multi": TEST_FIXTURES.multi_graph,
        }

    def test_gml_roundtrip_compatibility(self, networkx_graphs):
        """Test that GML files can be written and read back consistently."""
        for name, graph in networkx_graphs.items():
            if isinstance(graph, (nx.MultiGraph, nx.MultiDiGraph)):
                continue  # Skip MultiGraphs as GML doesn't handle them well

            # Write to temporary GML file
            temp_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".gml", delete=False
            )
            try:
                nx.write_gml(graph, temp_file.name, stringizer=str)

                # Read back and compare
                read_graph = nx.read_gml(temp_file.name, destringizer=None)

                assert read_graph.number_of_nodes() == graph.number_of_nodes()
                assert read_graph.number_of_edges() == graph.number_of_edges()

            finally:
                os.unlink(temp_file.name)

    def test_gexf_roundtrip_compatibility(self, networkx_graphs):
        """Test that GEXF files can be written and read back consistently."""
        for name, graph in networkx_graphs.items():
            # Write to temporary GEXF file
            temp_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".gexf", delete=False
            )
            try:
                nx.write_gexf(graph, temp_file.name)

                # Read back and compare
                read_graph = nx.read_gexf(temp_file.name)

                assert read_graph.number_of_nodes() == graph.number_of_nodes()
                assert read_graph.number_of_edges() == graph.number_of_edges()

            finally:
                os.unlink(temp_file.name)


class TestFileFormatAttributes:
    """Test attribute preservation across file formats."""

    def test_gml_attribute_preservation(self, kiara_api):
        """Test that GML files preserve node and edge attributes."""
        gml_content = """graph [
  node [
    id 0
    label "Node A"
    type "important"
    value 42.5
    active 1
  ]
  node [
    id 1
    label "Node B"
    type "normal"
    value 13.7
    active 0
  ]
  edge [
    source 0
    target 1
    weight 2.5
    relationship "connects"
    bidirectional 1
  ]
]"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write(gml_content)
        temp_file.flush()

        try:
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"]

            # Check node attributes
            nodes_df = network_data.data.nodes.to_polars_dataframe()
            expected_node_attrs = ["type", "value", "active"]
            for attr in expected_node_attrs:
                assert attr in nodes_df.columns

            # Check edge attributes
            edges_df = network_data.data.edges.to_polars_dataframe()
            expected_edge_attrs = ["weight", "relationship", "bidirectional"]
            for attr in expected_edge_attrs:
                assert attr in edges_df.columns

        finally:
            os.unlink(temp_file.name)

    def test_gexf_attribute_preservation(self, kiara_api):
        """Test that GEXF files preserve node and edge attributes."""
        gexf_content = """<?xml version='1.0' encoding='utf-8'?>
<gexf version="1.2" xmlns="http://www.gexf.net/1.2draft">
  <graph mode="static" defaultedgetype="directed">
    <attributes class="node">
      <attribute id="0" title="type" type="string" />
      <attribute id="1" title="value" type="double" />
    </attributes>
    <attributes class="edge">
      <attribute id="0" title="weight" type="double" />
      <attribute id="1" title="category" type="string" />
    </attributes>
    <nodes>
      <node id="0" label="A">
        <attvalues>
          <attvalue for="0" value="person" />
          <attvalue for="1" value="25.5" />
        </attvalues>
      </node>
      <node id="1" label="B">
        <attvalues>
          <attvalue for="0" value="organization" />
          <attvalue for="1" value="100.0" />
        </attvalues>
      </node>
    </nodes>
    <edges>
      <edge id="0" source="0" target="1">
        <attvalues>
          <attvalue for="0" value="3.14" />
          <attvalue for="1" value="business" />
        </attvalues>
      </edge>
    </edges>
  </graph>
</gexf>"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gexf", delete=False)
        temp_file.write(gexf_content)
        temp_file.flush()

        try:
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"]

            # Check node attributes
            nodes_df = network_data.data.nodes.to_polars_dataframe()
            assert "type" in nodes_df.columns
            assert "value" in nodes_df.columns

            # Check edge attributes
            edges_df = network_data.data.edges.to_polars_dataframe()
            assert "weight" in edges_df.columns
            assert "category" in edges_df.columns

        finally:
            os.unlink(temp_file.name)


class TestFileFormatEdgeCases:
    """Test edge cases in file format handling."""

    def test_single_node_file(self, kiara_api):
        """Test handling of files with single isolated node."""
        single_node_gml = """graph [
  node [
    id 0
    label "Lonely"
  ]
]"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write(single_node_gml)
        temp_file.flush()

        try:
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"]
            assert network_data.data.num_nodes == 1
            assert network_data.data.num_edges == 0

        finally:
            os.unlink(temp_file.name)

    def test_nodes_only_file(self, kiara_api):
        """Test handling of files with nodes but no edges."""
        nodes_only_gml = """graph [
  node [
    id 0
    label "A"
  ]
  node [
    id 1
    label "B"
  ]
  node [
    id 2
    label "C"
  ]
]"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write(nodes_only_gml)
        temp_file.flush()

        try:
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"]
            assert network_data.data.num_nodes == 3
            assert network_data.data.num_edges == 0

        finally:
            os.unlink(temp_file.name)

    def test_self_loop_handling(self, kiara_api):
        """Test handling of files with self-loops."""
        self_loop_gml = """graph [
  node [
    id 0
    label "Self"
  ]
  node [
    id 1
    label "Normal"
  ]
  edge [
    source 0
    target 0
  ]
  edge [
    source 0
    target 1
  ]
]"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write(self_loop_gml)
        temp_file.flush()

        try:
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"]
            assert network_data.data.num_nodes == 2
            assert network_data.data.num_edges == 2

            # Verify self-loop is preserved
            edges_df = network_data.data.edges.to_polars_dataframe()
            sources = edges_df["_source"].to_list()
            targets = edges_df["_target"].to_list()

            # Should have at least one self-loop
            self_loops = [i for i, (s, t) in enumerate(zip(sources, targets)) if s == t]
            assert len(self_loops) > 0

        finally:
            os.unlink(temp_file.name)

    def test_unicode_handling(self, kiara_api):
        """Test handling of Unicode characters in file content."""
        unicode_gml = """graph [
  node [
    id 0
    label "Nöde Ä"
    description "测试节点"
  ]
  node [
    id 1
    label "Nōdē Ē"
    description "тестовый узел"
  ]
  edge [
    source 0
    target 1
    label "连接"
  ]
]"""

        temp_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".gml", encoding="utf-8", delete=False
        )
        temp_file.write(unicode_gml)
        temp_file.flush()

        try:
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"]
            assert network_data.data.num_nodes == 2
            assert network_data.data.num_edges == 1

            # Verify Unicode characters are preserved
            nodes_df = network_data.data.nodes.to_polars_dataframe()
            labels = nodes_df["_label"].to_list()

            assert any("Ä" in label for label in labels)
            assert any("Ē" in label for label in labels)

        finally:
            os.unlink(temp_file.name)


class TestFormatSpecificFeatures:
    """Test format-specific features and limitations."""

    def test_gml_numeric_types(self, kiara_api):
        """Test GML handling of different numeric types."""
        numeric_gml = """graph [
  node [
    id 0
    label "Numbers"
    int_attr 42
    float_attr 3.14159
    sci_notation 1.23e-4
    negative_int -17
    negative_float -2.718
  ]
  edge [
    source 0
    target 0
    weight 0.5
    count 3
  ]
]"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write(numeric_gml)
        temp_file.flush()

        try:
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"]

            # Verify numeric attributes are preserved with correct types
            nodes_df = network_data.data.nodes.to_polars_dataframe()
            edges_df = network_data.data.edges.to_polars_dataframe()

            # Check that numeric columns exist
            numeric_node_attrs = [
                "int_attr",
                "float_attr",
                "sci_notation",
                "negative_int",
                "negative_float",
            ]
            for attr in numeric_node_attrs:
                assert attr in nodes_df.columns

            numeric_edge_attrs = ["weight", "count"]
            for attr in numeric_edge_attrs:
                assert attr in edges_df.columns

        finally:
            os.unlink(temp_file.name)

    def test_gexf_dynamic_attributes(self, kiara_api):
        """Test GEXF handling of dynamic attributes (if supported)."""
        # Note: This tests basic GEXF functionality since dynamic features
        # may not be fully supported by NetworkX
        static_gexf = """<?xml version='1.0' encoding='utf-8'?>
<gexf version="1.2" xmlns="http://www.gexf.net/1.2draft">
  <graph mode="static" defaultedgetype="undirected">
    <attributes class="node">
      <attribute id="0" title="size" type="integer" />
    </attributes>
    <nodes>
      <node id="0" label="A">
        <attvalues>
          <attvalue for="0" value="10" />
        </attvalues>
      </node>
      <node id="1" label="B">
        <attvalues>
          <attvalue for="0" value="20" />
        </attvalues>
      </node>
    </nodes>
    <edges>
      <edge id="0" source="0" target="1" />
    </edges>
  </graph>
</gexf>"""

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gexf", delete=False)
        temp_file.write(static_gexf)
        temp_file.flush()

        try:
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"]
            assert network_data.data.num_nodes == 2
            assert network_data.data.num_edges == 1

            # Verify attributes
            nodes_df = network_data.data.nodes.to_polars_dataframe()
            assert "size" in nodes_df.columns

        finally:
            os.unlink(temp_file.name)


class TestFileFormatIntegration:
    """Test integration with other kiara operations."""

    def test_file_import_to_networkx_export(self, kiara_api):
        """Test importing file and exporting to NetworkX."""
        gml_content = create_test_file_content()["simple.gml"]

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write(gml_content)
        temp_file.flush()

        try:
            # Import from file
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"].data

            # Convert to NetworkX
            nx_graph = network_data.as_networkx_graph(
                nx.Graph, incl_node_attributes=True
            )

            assert isinstance(nx_graph, nx.Graph)
            assert nx_graph.number_of_nodes() == 3
            assert nx_graph.number_of_edges() == 3

            # Verify attributes are preserved
            for node_id in nx_graph.nodes():
                node_data = nx_graph.nodes[node_id]
                assert "_label" in node_data

        finally:
            os.unlink(temp_file.name)

    def test_file_import_metadata_computation(self, kiara_api):
        """Test that imported files have proper metadata computed."""
        gml_content = create_test_file_content()["simple.gml"]

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".gml", delete=False)
        temp_file.write(gml_content)
        temp_file.flush()

        try:
            result = kiara_api.run_job(
                operation="create.network_data.from.file",
                inputs={"file": temp_file.name},
            )

            network_data = result["network_data"].data

            # Should have component IDs computed
            component_ids = network_data.component_ids
            assert len(component_ids) >= 1

            # Should be able to query the data
            nodes_result = network_data.query_nodes(
                "SELECT COUNT(*) as count FROM nodes"
            )
            node_count = nodes_result.column(0)[0].as_py()
            assert node_count == 3

        finally:
            os.unlink(temp_file.name)
