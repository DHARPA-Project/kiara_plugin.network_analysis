# -*- coding: utf-8 -*-
from typing import Any, Dict

from kiara.api import KiaraModule, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraException
from kiara_plugin.network_analysis.defaults import (
    ATTRIBUTE_PROPERTY_KEY,
    COMPONENT_ID_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.network_analysis.models.metadata import NetworkNodeAttributeMetadata

KIARA_METADATA = {
    "authors": [
        {"name": "Lena Jaskov", "email": "helena.jaskov@uni.lu"},
        {"name": "Markus Binsteiner", "email": "markus@frkl.io"},
    ],
    "description": "Modules related to extracting components from network data.",
}

COMPONENT_COLUMN_TEXT = """The id of the component the node is part of.

If all nodes are connected, all nodes will have '0' as value in the component_id field. Otherwise, the nodes will be assigned 'component_id'-s according to the component they belong to, with the largest component having '0' as component_id, the second largest '1' and so on. If two components have the same size, who gets the higher component_id is not determinate."""
COMPONENT_COLUMN_METADATA = NetworkNodeAttributeMetadata(doc=COMPONENT_COLUMN_TEXT)  # type: ignore


class ExtractLargestComponentModule(KiaraModule):
    """Extract the largest connected component from this network data.

    This module analyses network data and checks if it contains clusters, and if so, how many. If all nodes are connected, all nodes will have '0' as value in the component_id field.

    Otherwise, the nodes will be assigned 'component_id'-s according to the component they belong to, with the  largest component having '0' as component_id, the second largest '1' and so on. If two components have the same size, who gets the higher component_id is not determinate.
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

        # TODO: maybe this can be done directly in sql, without networx, which would be faster and better
        # for memory usage
        undir_graph = network_data.as_rustworkx_graph(
            graph_type=rx.PyGraph,
            multigraph=False,
            omit_self_loops=False,
            attach_node_id_map=True,
        )
        undir_components = rx.connected_components(undir_graph)

        nodes_columns_metadata = {
            COMPONENT_ID_COLUMN_NAME: {
                ATTRIBUTE_PROPERTY_KEY: COMPONENT_COLUMN_METADATA
            }
        }

        if len(undir_components) == 1:

            nodes = network_data.nodes.arrow_table
            components_column = pa.array([0] * len(nodes), type=pa.int64())
            nodes = nodes.append_column(COMPONENT_ID_COLUMN_NAME, components_column)

            network_data = NetworkData.create_network_data(
                nodes_table=nodes,
                edges_table=network_data.edges.arrow_table,
                augment_tables=False,
                nodes_column_metadata=nodes_columns_metadata,
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
        nodes = nodes.append_column(COMPONENT_ID_COLUMN_NAME, components_column)
        network_data = NetworkData.create_network_data(
            nodes_table=nodes,
            edges_table=network_data.edges.arrow_table,
            augment_tables=False,
            nodes_column_metadata=nodes_columns_metadata,
        )
        outputs.set_values(
            is_connected=is_connected,
            number_of_components=number_of_components,
            network_data=network_data,
        )