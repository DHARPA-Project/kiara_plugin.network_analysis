#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test data fixtures and utilities for network analysis plugin tests."""

import tempfile
from pathlib import Path
from typing import Dict, Tuple

import networkx as nx
import pyarrow as pa

from kiara_plugin.network_analysis.models import NetworkData


def create_simple_graph() -> nx.Graph:
    """Create a simple undirected graph for testing."""
    G = nx.Graph()
    G.add_nodes_from(
        [
            (0, {"label": "A", "type": "person", "age": 25}),
            (1, {"label": "B", "type": "person", "age": 30}),
            (
                2,
                {"label": "C", "type": "organization", "age": None},
            ),  # Consistent attributes
            (3, {"label": "D", "type": "person", "age": 35}),
        ]
    )
    G.add_edges_from(
        [
            (0, 1, {"weight": 1.0, "relationship": "friend"}),
            (1, 2, {"weight": 2.5, "relationship": "works_for"}),
            (2, 3, {"weight": 1.2, "relationship": "employs"}),
            (0, 3, {"weight": 0.8, "relationship": "colleague"}),
        ]
    )
    return G


def create_simple_digraph() -> nx.DiGraph:
    """Create a simple directed graph for testing."""
    G = nx.DiGraph()
    G.add_nodes_from(
        [
            (0, {"label": "Root", "level": 0}),
            (1, {"label": "Child1", "level": 1}),
            (2, {"label": "Child2", "level": 1}),
            (3, {"label": "Grandchild", "level": 2}),
        ]
    )
    G.add_edges_from(
        [
            (0, 1, {"edge_type": "parent_child", "strength": 1.0}),
            (0, 2, {"edge_type": "parent_child", "strength": 0.9}),
            (1, 3, {"edge_type": "parent_child", "strength": 0.8}),
            (2, 3, {"edge_type": "parent_child", "strength": 0.7}),
        ]
    )
    return G


def create_multi_graph() -> nx.MultiGraph:
    """Create a multi-graph with parallel edges for testing."""
    G = nx.MultiGraph()
    G.add_nodes_from(
        [
            (0, {"label": "Station A", "city": "New York"}),
            (1, {"label": "Station B", "city": "Boston"}),
            (2, {"label": "Station C", "city": "Chicago"}),
        ]
    )
    # Add parallel edges between same nodes
    G.add_edge(0, 1, key="train", transport="train", duration=4.5)
    G.add_edge(0, 1, key="bus", transport="bus", duration=6.0)
    G.add_edge(0, 1, key="flight", transport="flight", duration=1.5)
    G.add_edge(1, 2, key="train", transport="train", duration=3.0)
    G.add_edge(0, 2, key="flight", transport="flight", duration=2.0)
    return G


def create_disconnected_graph() -> nx.Graph:
    """Create a graph with multiple components for testing."""
    G = nx.Graph()
    # Component 1 (triangle)
    G.add_nodes_from(
        [
            (0, {"label": "A1", "component": "first"}),
            (1, {"label": "B1", "component": "first"}),
            (2, {"label": "C1", "component": "first"}),
        ]
    )
    G.add_edges_from(
        [
            (0, 1, {"weight": 1.0}),
            (1, 2, {"weight": 1.0}),
            (2, 0, {"weight": 1.0}),
        ]
    )

    # Component 2 (line)
    G.add_nodes_from(
        [
            (3, {"label": "A2", "component": "second"}),
            (4, {"label": "B2", "component": "second"}),
        ]
    )
    G.add_edge(3, 4, weight=2.0)

    # Isolated node
    G.add_node(5, label="Isolated", component="third")

    return G


def create_bipartite_graph() -> nx.Graph:
    """Create a bipartite graph for testing."""
    G = nx.Graph()
    # Set 1 (people)
    people = [
        (0, {"label": "Alice", "type": "person", "bipartite": 0}),
        (1, {"label": "Bob", "type": "person", "bipartite": 0}),
        (2, {"label": "Charlie", "type": "person", "bipartite": 0}),
    ]
    # Set 2 (projects)
    projects = [
        (10, {"label": "Project X", "type": "project", "bipartite": 1}),
        (11, {"label": "Project Y", "type": "project", "bipartite": 1}),
        (12, {"label": "Project Z", "type": "project", "bipartite": 1}),
    ]

    G.add_nodes_from(people + projects)
    G.add_edges_from(
        [
            (0, 10, {"role": "lead"}),  # Alice -> Project X
            (0, 11, {"role": "contributor"}),  # Alice -> Project Y
            (1, 10, {"role": "contributor"}),  # Bob -> Project X
            (1, 12, {"role": "lead"}),  # Bob -> Project Z
            (2, 11, {"role": "lead"}),  # Charlie -> Project Y
            (2, 12, {"role": "contributor"}),  # Charlie -> Project Z
        ]
    )
    return G


def create_self_loop_graph() -> nx.Graph:
    """Create a graph with self-loops for testing."""
    G = nx.Graph()
    G.add_nodes_from(
        [
            (0, {"label": "Self", "reflexive": True}),
            (1, {"label": "Normal", "reflexive": False}),
            (2, {"label": "Also_Self", "reflexive": True}),
        ]
    )
    G.add_edges_from(
        [
            (0, 0, {"type": "self_loop"}),  # Self-loop
            (0, 1, {"type": "normal"}),
            (1, 2, {"type": "normal"}),
            (2, 2, {"type": "self_loop"}),  # Another self-loop
        ]
    )
    return G


def create_arrow_tables() -> Tuple[pa.Table, pa.Table]:
    """Create simple PyArrow tables for direct NetworkData creation."""
    # Nodes table
    nodes_data = {
        "_node_id": [0, 1, 2, 3],
        "_label": ["Alpha", "Beta", "Gamma", "Delta"],
        "category": ["A", "B", "A", "C"],
        "value": [10.5, 20.0, 15.2, 8.7],
    }
    nodes_table = pa.Table.from_pydict(nodes_data)

    # Edges table
    edges_data = {
        "_source": [0, 1, 2, 0, 1],
        "_target": [1, 2, 3, 3, 0],
        "weight": [1.0, 2.0, 1.5, 0.8, 3.0],
        "edge_type": ["A", "B", "A", "C", "B"],
    }
    edges_table = pa.Table.from_pydict(edges_data)

    return nodes_table, edges_table


def create_network_data_from_graph(graph: nx.Graph) -> NetworkData:
    """Convert a NetworkX graph to NetworkData for testing."""
    return NetworkData.create_from_networkx_graph(graph)


def create_test_file_content() -> Dict[str, str]:
    """Create content for various network file formats."""
    return {
        "simple.gml": """graph [
  node [
    id 0
    label "A"
    value 1.0
  ]
  node [
    id 1
    label "B"
    value 2.0
  ]
  node [
    id 2
    label "C"
    value 3.0
  ]
  edge [
    source 0
    target 1
    weight 1.5
  ]
  edge [
    source 1
    target 2
    weight 2.5
  ]
  edge [
    source 0
    target 2
    weight 0.5
  ]
]""",
        "simple.gexf": """<?xml version='1.0' encoding='utf-8'?>
<gexf version="1.2" xmlns="http://www.gexf.net/1.2draft">
  <graph mode="static" defaultedgetype="undirected">
    <attributes class="node">
      <attribute id="0" title="value" type="float" />
    </attributes>
    <attributes class="edge">
      <attribute id="0" title="weight" type="float" />
    </attributes>
    <nodes>
      <node id="0" label="A">
        <attvalues>
          <attvalue for="0" value="1.0" />
        </attvalues>
      </node>
      <node id="1" label="B">
        <attvalues>
          <attvalue for="0" value="2.0" />
        </attvalues>
      </node>
      <node id="2" label="C">
        <attvalues>
          <attvalue for="0" value="3.0" />
        </attvalues>
      </node>
    </nodes>
    <edges>
      <edge id="0" source="0" target="1">
        <attvalues>
          <attvalue for="0" value="1.5" />
        </attvalues>
      </edge>
      <edge id="1" source="1" target="2">
        <attvalues>
          <attvalue for="0" value="2.5" />
        </attvalues>
      </edge>
      <edge id="2" source="0" target="2">
        <attvalues>
          <attvalue for="0" value="0.5" />
        </attvalues>
      </edge>
    </edges>
  </graph>
</gexf>""",
        "nodes.csv": """id,label,type,value
0,Alice,person,1.0
1,Bob,person,2.0
2,Charlie,organization,3.0""",
        "edges.csv": """source,target,weight,relationship
0,1,1.5,friend
1,2,2.5,colleague
0,2,0.5,acquaintance""",
    }


def create_temporary_files() -> Dict[str, Path]:
    """Create temporary files with test network data."""
    file_contents = create_test_file_content()
    temp_files = {}

    for filename, content in file_contents.items():
        temp_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=f".{filename.split('.')[-1]}", delete=False
        )
        temp_file.write(content)
        temp_file.flush()
        temp_files[filename] = Path(temp_file.name)

    return temp_files


class NetworkDataTestFixtures:
    """Container class for all test network data."""

    def __init__(self):
        self.simple_graph = create_simple_graph()
        self.simple_digraph = create_simple_digraph()
        self.multi_graph = create_multi_graph()
        self.disconnected_graph = create_disconnected_graph()
        self.bipartite_graph = create_bipartite_graph()
        self.self_loop_graph = create_self_loop_graph()

        self.simple_network_data = create_network_data_from_graph(self.simple_graph)
        self.digraph_network_data = create_network_data_from_graph(self.simple_digraph)
        self.multi_network_data = create_network_data_from_graph(self.multi_graph)
        self.disconnected_network_data = create_network_data_from_graph(
            self.disconnected_graph
        )
        self.bipartite_network_data = create_network_data_from_graph(
            self.bipartite_graph
        )
        self.self_loop_network_data = create_network_data_from_graph(
            self.self_loop_graph
        )

        self.nodes_table, self.edges_table = create_arrow_tables()
        self.arrow_network_data = NetworkData.create_network_data(
            self.nodes_table, self.edges_table
        )

    def get_all_network_data(self) -> Dict[str, NetworkData]:
        """Get all NetworkData instances for parameterized testing."""
        return {
            "simple": self.simple_network_data,
            "digraph": self.digraph_network_data,
            "multi": self.multi_network_data,
            "disconnected": self.disconnected_network_data,
            "bipartite": self.bipartite_network_data,
            "self_loop": self.self_loop_network_data,
            "arrow": self.arrow_network_data,
        }

    def get_all_networkx_graphs(self) -> Dict[str, nx.Graph]:
        """Get all NetworkX graphs for parameterized testing."""
        return {
            "simple": self.simple_graph,
            "digraph": self.simple_digraph,
            "multi": self.multi_graph,
            "disconnected": self.disconnected_graph,
            "bipartite": self.bipartite_graph,
            "self_loop": self.self_loop_graph,
        }


# Global fixture instance
TEST_FIXTURES = NetworkDataTestFixtures()
