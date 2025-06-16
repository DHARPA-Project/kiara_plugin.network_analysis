#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_plugin.develop` package."""
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

import networkx as nx
import rustworkx as rx

import pytest  # noqa

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
EXAMPLES_DIR = Path(os.path.join(ROOT_DIR, "examples"))
DATA_DIR = Path(os.path.join(EXAMPLES_DIR, "data"))

if TYPE_CHECKING:
    from kiara.interfaces import BaseAPI


def test_network_datas(example_data: Dict[str, Any]):


    networkx_graph = example_data["networkx_graph"]
    networkx_digraph = example_data["networkx_digraph"]

    kiara_network_data = example_data["kiara_network_data"]


    kiara_networkx_graph = kiara_network_data.data.as_networkx_graph(nx.Graph)
    kiara_networkx_digraph = kiara_network_data.data.as_networkx_graph(nx.DiGraph)
    kiara_rustworkx_graph = kiara_network_data.data.as_rustworkx_graph(rx.PyGraph)
    kiara_rustworkx_digraph = kiara_network_data.data.as_rustworkx_graph(rx.PyDiGraph)

    assert networkx_graph.number_of_nodes() == kiara_networkx_graph.number_of_nodes()
    assert networkx_graph.number_of_nodes() == kiara_rustworkx_graph.num_nodes()

    assert networkx_graph.number_of_edges() == kiara_networkx_graph.number_of_edges()
    assert networkx_graph.number_of_edges() == kiara_rustworkx_graph.num_edges()

    assert networkx_digraph.number_of_nodes() == kiara_networkx_digraph.number_of_nodes()
    assert networkx_digraph.number_of_nodes() == kiara_rustworkx_digraph.num_nodes()

    assert networkx_digraph.number_of_edges() == kiara_networkx_digraph.number_of_edges()
    assert networkx_digraph.number_of_edges() == kiara_rustworkx_digraph.num_edges()


