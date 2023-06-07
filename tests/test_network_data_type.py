# -*- coding: utf-8 -*-
from typing import Mapping

import networkx as nx
import pyarrow as pa

from kiara_plugin.network_analysis.defaults import (
    NODE_ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.utils import (
    extract_edges_as_table,
    extract_nodes_as_table,
)


def test_extract_nodes_as_table():
    # Create a simple graph with attributes
    G = nx.DiGraph()
    G.add_node(1, attr1="value1", attr2=42)
    G.add_node(2, attr1="value2", attr2=84)

    # Test the extract_nodes_as_table function
    result_table, result_mapping = extract_nodes_as_table(G)

    # Check if the result is a tuple
    assert isinstance(result_table, pa.Table)
    assert isinstance(result_mapping, Mapping)

    # Check if the result table has the expected columns
    expected_columns = [NODE_ID_COLUMN_NAME, LABEL_COLUMN_NAME, "attr1", "attr2"]
    for column in expected_columns:
        assert column in result_table.schema.names

    # Check if the result mapping has the expected keys and values
    expected_mapping = {1: 0, 2: 1}
    assert result_mapping == expected_mapping

    # Check if the result table has the expected data
    assert result_table.column(NODE_ID_COLUMN_NAME).to_pylist() == [0, 1]
    assert result_table.column(LABEL_COLUMN_NAME).to_pylist() == ["1", "2"]
    assert result_table.column("attr1").to_pylist() == ["value1", "value2"]
    assert result_table.column("attr2").to_pylist() == [42, 84]


def test_extract_edges_as_table():
    # Create a simple graph with edges and their attributes
    G = nx.DiGraph()
    G.add_edge(1, 2, attr1="value1", attr2=42)
    G.add_edge(2, 3, attr1="value2", attr2=84)

    # Create a node_id_map
    node_id_map = {1: 0, 2: 1, 3: 2}

    # Test the extract_edges_as_table function
    result_table = extract_edges_as_table(G, node_id_map)

    # Check if the result table has the expected columns
    expected_columns = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME, "attr1", "attr2"]
    for column in expected_columns:
        assert column in result_table.schema.names

    # Check if the result table has the expected data
    assert result_table.column(SOURCE_COLUMN_NAME).to_pylist() == [0, 1]
    assert result_table.column(TARGET_COLUMN_NAME).to_pylist() == [1, 2]
    assert result_table.column("attr1").to_pylist() == ["value1", "value2"]
    assert result_table.column("attr2").to_pylist() == [42, 84]
