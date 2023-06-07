# -*- coding: utf-8 -*-
from typing import Any, Dict

from kiara.api import KiaraModule, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraException
from kiara_plugin.network_analysis.models import NetworkData

KIARA_METADATA = {
    "authors": [
        {"name": "Lena Jaskov", "email": "helena.jaskov@uni.lu"},
        {"name": "Markus Binsteiner", "email": "markus@frkl.io"},
    ],
    "description": "Modules related to extracting components from network data.",
}


class ExtractLargestComponentModule(KiaraModule):
    """Extract the largest connected component from this network data.

    This module analyses network data and checks if it contains clusters, and if so, how many. If all nodes are connected
    to each other, the input data will be returned as largest component and the 'other_components' output will be unset.

    Otherwise, the dataset will be split up into nodes of the largest component, and nodes that are not part of that.
    Then this module will create 2 new network data items, one for the largest component, and one for the other components that excludes
    the nodes and edges that are part of the largest component.
    """

    _module_type_name = "network_data.extract_components"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:
        result = {
            "network_data": {
                "type": "network_data",
                "doc": "The network data to analyze.",
            }
        }
        return result

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:
        result: Dict[str, Dict[str, Any]] = {}

        result["network_data"] = {
            "type": "network_data",
            "doc": "The network_data, with a new column added to the nodes table, indicating the component the node belongs to.",
        }

        result["number_of_components"] = {
            "type": "integer",
            "doc": "The number of components in the graph.",
        }

        result["is_connected"] = {
            "type": "boolean",
            "doc": "Whether the graph is connected or not.",
        }
        return result

    def process(self, inputs: ValueMap, outputs: ValueMap):
        import pyarrow as pa
        import rustworkx as rx

        network_value = inputs.get_value_obj("network_data")
        network_data: NetworkData = network_value.data

        component_column_name = "component"

        # TODO: maybe this can be done directly in sql, without networx, which would be faster and better
        # for memory usage
        undir_graph = network_data.as_rustworkx_graph(
            graph_type=rx.PyGraph,
            multigraph=False,
            omit_self_loops=False,
            attach_node_id_map=True,
        )
        undir_components = rx.connected_components(undir_graph)

        if len(undir_components) == 1:

            nodes = network_data.nodes.arrow_table
            components_column = pa.array([0] * len(nodes), type=pa.int64())
            nodes = nodes.append_column(component_column_name, components_column)

            network_data = NetworkData.create_network_data(
                nodes_table=nodes,
                edges_table=network_data.edges.arrow_table,
                augment_tables=False,
            )
            outputs.set_values(
                network_data=network_data,
                number_of_components=1,
                is_connected=True,
            )
            return

        number_of_components = len(undir_components)
        is_connected = False
        node_id_map = undir_graph.attrs["node_id_map"]

        node_components = {}
        for idx, component in enumerate(
            sorted(undir_components, key=len, reverse=True)
        ):
            for node in component:
                node_id = node_id_map[node]
                node_components[node_id] = idx

        if len(node_components) != network_data.num_nodes:
            raise KiaraException(
                "Number of nodes in component map does not match number of nodes in network data. This is most likely a bug."
            )

        components_column = pa.array(
            (node_components[node_id] for node_id in sorted(node_components.keys())),
            type=pa.int64(),
        )

        nodes = network_data.nodes.arrow_table
        nodes = nodes.append_column(component_column_name, components_column)
        network_data = NetworkData.create_network_data(
            nodes_table=nodes,
            edges_table=network_data.edges.arrow_table,
            augment_tables=False,
        )
        outputs.set_values(
            is_connected=is_connected,
            number_of_components=number_of_components,
            network_data=network_data,
        )
