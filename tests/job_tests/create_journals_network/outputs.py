# -*- coding: utf-8 -*-
from kiara.models.values.value import Value
from kiara_plugin.network_analysis.models import NetworkGraphProperties


def check_properties(network_data: Value):

    properties: NetworkGraphProperties = network_data.get_property_data(
        "metadata.network_data"
    )
    assert (
        properties.number_of_nodes == 276
    ), f"Invalid number of nodes: {properties.number_of_nodes} != 276"
