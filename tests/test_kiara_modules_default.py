#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_plugin.network_analysis` package."""
from pathlib import Path

import pytest  # noqa
from kiara.interfaces.python_api import KiaraAPI
from kiara.models.values.value import Value

import kiara_plugin.network_analysis
from kiara_plugin.network_analysis.models import (
    GraphType,
    NetworkData,
    NetworkGraphProperties,
)


def test_assert():

    assert kiara_plugin.network_analysis.get_version() is not None


def test_network_data_properties(kiara_api: KiaraAPI, data_folder: Path):

    assert kiara_api.list_operations()
    example_dir = data_folder / "simple_networks" / "example_1"

    inputs = {
        "edges_file": (example_dir / "SampleEdges.csv").as_posix(),
        "nodes_file": (example_dir / "SampleNodes.csv").as_posix(),
        "id_column_name": "Id",
    }

    result = kiara_api.run_job("create.network_data.from.files", inputs=inputs)

    nd: Value = result["network_data"]

    assert nd.data_type_name == "network_data"
    properties: NetworkGraphProperties = nd.get_all_property_data()[
        "metadata.graph_properties"
    ]

    assert properties.number_of_nodes == 4
    assert properties.number_of_self_loops == 2
    assert properties.number_of_parallel_edges == 3

    properties_by_graph_type = properties.properties_by_graph_type
    assert len(properties_by_graph_type) == 4
    for p in properties_by_graph_type:
        if p.graph_type == GraphType.DIRECTED:
            assert p.number_of_edges == 8
        elif p.graph_type == GraphType.UNDIRECTED:
            assert p.number_of_edges == 6
        elif p.graph_type == GraphType.DIRECTED_MULTI:
            assert p.number_of_edges == 11
        elif p.graph_type == GraphType.UNDIRECTED_MULTI:
            assert p.number_of_edges == 11
        else:
            assert False, f"Invalid graph type: '{p.number_of_edges}'."

    inputs = {
        "network_data": nd,
    }
    result = kiara_api.run_job("network_data.check_clusters", inputs=inputs)

    is_connected = result.get_value_data("is_connected")
    assert is_connected is True
    assert result.get_value_data("number_of_components") == 1

    largest_component_value = result.get_value_obj("largest_component")
    assert largest_component_value.data_type_name == "network_data"

    component_network = largest_component_value.data
    assert isinstance(component_network, NetworkData)

    # TODO: should this be the same as the original network?
    # properties: NetworkGraphProperties = largest_component_value.get_all_property_data()["metadata.graph_properties"]
    #
    # assert properties.number_of_nodes == 4
    # assert properties.number_of_self_loops == 2
    # assert properties.number_of_parallel_edges == 0
    #
    # properties_by_graph_type = properties.properties_by_graph_type
    # assert len(properties_by_graph_type) == 4
    # for p in properties_by_graph_type:
    #     if p.graph_type == GraphType.DIRECTED:
    #         assert p.number_of_edges == 6
    #     elif p.graph_type == GraphType.UNDIRECTED:
    #         assert p.number_of_edges == 6
    #     elif p.graph_type == GraphType.DIRECTED_MULTI:
    #         assert p.number_of_edges == 6
    #     elif p.graph_type == GraphType.UNDIRECTED_MULTI:
    #         assert p.number_of_edges == 6
    #     else:
    #         assert False, f"Invalid graph type: '{p.number_of_edges}'."
