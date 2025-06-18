#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_plugin.develop` package."""

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

import networkx as nx
import pytest  # noqa
import rustworkx as rx

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
EXAMPLES_DIR = Path(os.path.join(ROOT_DIR, "examples"))
DATA_DIR = Path(os.path.join(EXAMPLES_DIR, "data"))

if TYPE_CHECKING:
    pass


def test_network_datas(example_data: Dict[str, Any]):
    networkx_graph = example_data.get("networkx_graph", None)
    networkx_digraph = example_data.get("networkx_digraph", None)
    networkx_multigraph = example_data.get("networkx_multigraph", None)
    networkx_multidigraph = example_data.get("networkx_multidigraph", None)

    kiara_network_data = example_data["kiara_network_data"]

    metadata = kiara_network_data.property_values["metadata.network_data"].data

    if networkx_graph is not None:
        kiara_networkx_graph = kiara_network_data.data.as_networkx_graph(nx.Graph)
        kiara_rustworkx_graph = kiara_network_data.data.as_rustworkx_graph(rx.PyGraph)

        assert networkx_graph.number_of_nodes() == metadata.number_of_nodes
        assert (
            networkx_graph.number_of_nodes() == kiara_networkx_graph.number_of_nodes()
        )
        assert networkx_graph.number_of_nodes() == kiara_rustworkx_graph.num_nodes()

        assert (
            networkx_graph.number_of_edges()
            == metadata.properties_by_graph_type["undirected"].number_of_edges
        )
        assert (
            networkx_graph.number_of_edges() == kiara_networkx_graph.number_of_edges()
        )
        assert networkx_graph.number_of_edges() == kiara_rustworkx_graph.num_edges()

    if networkx_digraph is not None:
        kiara_networkx_digraph = kiara_network_data.data.as_networkx_graph(nx.DiGraph)
        kiara_rustworkx_digraph = kiara_network_data.data.as_rustworkx_graph(
            rx.PyDiGraph
        )

        assert networkx_graph.number_of_nodes() == metadata.number_of_nodes
        assert (
            networkx_digraph.number_of_nodes()
            == kiara_networkx_digraph.number_of_nodes()
        )
        assert networkx_digraph.number_of_nodes() == kiara_rustworkx_digraph.num_nodes()

        assert (
            networkx_digraph.number_of_edges()
            == metadata.properties_by_graph_type["directed"].number_of_edges
        )
        assert (
            networkx_digraph.number_of_edges()
            == kiara_networkx_digraph.number_of_edges()
        )
        assert networkx_digraph.number_of_edges() == kiara_rustworkx_digraph.num_edges()

    if networkx_multigraph is not None:
        kiara_networkx_multigraph = kiara_network_data.data.as_networkx_graph(
            nx.MultiGraph
        )
        kiara_rustworkx_multigraph = kiara_network_data.data.as_rustworkx_graph(
            rx.PyGraph, multigraph=True
        )

        assert networkx_graph.number_of_nodes() == metadata.number_of_nodes
        assert (
            networkx_multigraph.number_of_nodes()
            == kiara_networkx_multigraph.number_of_nodes()
        )
        assert (
            networkx_multigraph.number_of_nodes()
            == kiara_rustworkx_multigraph.num_nodes()
        )

        assert (
            networkx_multigraph.number_of_edges()
            == metadata.properties_by_graph_type["undirected_multi"].number_of_edges
        )
        assert (
            networkx_multigraph.number_of_edges()
            == kiara_networkx_multigraph.number_of_edges()
        )
        assert (
            networkx_multigraph.number_of_edges()
            == kiara_rustworkx_multigraph.num_edges()
        )

    if networkx_multidigraph is not None:
        kiara_networkx_multidigraph = kiara_network_data.data.as_networkx_graph(
            nx.MultiDiGraph
        )
        kiara_rustworkx_multidigraph = kiara_network_data.data.as_rustworkx_graph(
            rx.PyDiGraph, multigraph=True
        )

        assert networkx_graph.number_of_nodes() == metadata.number_of_nodes
        assert (
            networkx_multidigraph.number_of_nodes()
            == kiara_networkx_multidigraph.number_of_nodes()
        )
        assert (
            networkx_multidigraph.number_of_nodes()
            == kiara_rustworkx_multidigraph.num_nodes()
        )

        assert (
            networkx_multidigraph.number_of_edges()
            == metadata.properties_by_graph_type["directed_multi"].number_of_edges
        )
        assert (
            networkx_multidigraph.number_of_edges()
            == kiara_networkx_multidigraph.number_of_edges()
        )
        assert (
            networkx_multidigraph.number_of_edges()
            == kiara_rustworkx_multidigraph.num_edges()
        )
