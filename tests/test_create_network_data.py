# -*- coding: utf-8 -*-
import os
from pathlib import Path

import networkx as nx

from kiara.models.values.value import Value
from kiara_plugin.network_analysis.defaults import (
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData, NetworkGraphProperties

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_FOLDER = Path(os.path.join(ROOT_DIR, "examples", "data"))


def assert_network_data_properties(
    value: Value,
    num_nodes: int,
    num_self_loops: int,
    num_parallel_edges: int,
    num_edges_undirected: int,
):

    properties = value.get_all_property_data()

    assert "metadata.graph_properties" in properties.keys()
    assert "metadata.database" in properties.keys()

    nd_props: NetworkGraphProperties = properties["metadata.graph_properties"]
    nd_props.number_of_self_loops = num_self_loops
    nd_props.number_of_parallel_edges = num_parallel_edges
    nd_props.number_of_nodes = num_nodes

    network_data: NetworkData = value.data
    g = network_data.as_networkx_graph(nx.Graph)
    assert len(g.nodes()) == num_nodes
    assert len(g.edges()) == num_edges_undirected

    assert SOURCE_COLUMN_NAME in network_data.edges_schema.columns.keys()
    assert TARGET_COLUMN_NAME in network_data.edges_schema.columns.keys()


def test_create_network_data_from_gexf(kiara_api):

    gexf_file = DATA_FOLDER / "gexf" / "quakers.gexf"
    op = "create.network_data.from.file"
    inputs = {"file": gexf_file.as_posix()}

    result = kiara_api.run_job(op, inputs=inputs)
    value = result.get_value_obj("network_data")

    assert_network_data_properties(value, 119, 0, 0, 174)
    network_data: NetworkData = value.data

    assert (
        "id" in network_data.edges_schema.columns.keys()
    )  # special to this network data


def test_create_network_data_from_gml(kiara_api):

    gml = DATA_FOLDER / "gml" / "karate.gml"
    op = "create.network_data.from.file"
    inputs = {"file": gml.as_posix()}

    result = kiara_api.run_job(op, inputs=inputs)
    value = result.get_value_obj("network_data")

    assert_network_data_properties(value, 34, 0, 0, 78)
