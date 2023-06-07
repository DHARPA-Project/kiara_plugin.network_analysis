# -*- coding: utf-8 -*-
from typing import Any, Dict, Tuple

from sqlalchemy import bindparam, text

from kiara.api import KiaraModule, ValueMap, ValueMapSchema
from kiara_plugin.network_analysis.defaults import (
    NODE_ID_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    NetworkDataTableType,
)
from kiara_plugin.network_analysis.models import NetworkData

KIARA_METADATA = {
    "authors": [
        {"name": "Lena Jaskov", "email": "helena.jaskov@uni.lu"},
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

    _module_type_name = "network_data.extract_largest_component"

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

        result["largest_component"] = {
            "type": "network_data",
            "doc": "A sub-graph of the largest component of the graph. In case the graph is a single component, the original input will be returned.",
        }

        result["other_components"] = {
            "type": "network_data",
            "doc": "A sub-graph of the other components of the graph, excluding the nodes (and edges referencing those nodes) contained in the largest component.",
            "optional": True,
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
        import networkx as nx

        network_value = inputs.get_value_obj("network_data")
        network_data: NetworkData = network_value.data

        # TODO: maybe this can be done directly in sql, without networx, which would be faster and better
        # for memory usage
        undir_graph = network_data.as_networkx_graph(nx.Graph)
        undir_components = list(nx.connected_components(undir_graph))

        if len(undir_components) == 1:
            outputs.set_values(
                largest_component=network_value,
                number_of_components=1,
                is_connected=True,
                other_components=None,
            )
            return

        nodes_largest_component: Tuple[Any, ...] = tuple(max(undir_components, key=len))  # type: ignore

        largest_component = network_data.clone()
        largest_component._unlock_db()

        with largest_component.get_sqlalchemy_engine().connect() as con:
            delete_from_nodes = text(
                f"""DELETE FROM {NetworkDataTableType.NODES.value} WHERE {NODE_ID_COLUMN_NAME} NOT IN :nodes"""
            )
            delete_from_nodes = delete_from_nodes.bindparams(
                bindparam("nodes", expanding=True, value=nodes_largest_component)
            )
            con.execute(delete_from_nodes)

            delete_from_edges = text(
                f"""DELETE FROM {NetworkDataTableType.EDGES.value} WHERE {SOURCE_COLUMN_NAME} NOT IN :{NetworkDataTableType.NODES.value} AND {TARGET_COLUMN_NAME} NOT IN :nodes"""
            )
            delete_from_edges = delete_from_edges.bindparams(
                bindparam("nodes", expanding=True, value=nodes_largest_component)
            )

            con.execute(delete_from_edges)
            con.commit()

        largest_component._lock_db()
        largest_component._invalidate()

        outputs.set_value("largest_component", largest_component)

        other_components = network_data.clone()
        other_components._unlock_db()
        with other_components.get_sqlalchemy_engine().connect() as con:
            delete_from_nodes = text(
                f"""DELETE FROM {NetworkDataTableType.NODES.value} WHERE {NODE_ID_COLUMN_NAME} IN :nodes"""
            )
            delete_from_nodes = delete_from_nodes.bindparams(
                bindparam("nodes", expanding=True, value=nodes_largest_component)
            )
            con.execute(delete_from_nodes)

            delete_from_edges = text(
                f"""DELETE FROM {NetworkDataTableType.EDGES.value} WHERE {SOURCE_COLUMN_NAME} IN :nodes OR {TARGET_COLUMN_NAME} IN :nodes"""
            )
            delete_from_edges = delete_from_edges.bindparams(
                bindparam("nodes", expanding=True, value=nodes_largest_component)
            )
            con.execute(delete_from_edges)

            con.commit()

        other_components._lock_db()
        other_components._invalidate()
        outputs.set_value("other_components", other_components)

        number_of_components = nx.number_connected_components(undir_graph)
        is_connected = number_of_components == 1
        outputs.set_values(
            is_connected=is_connected, number_of_components=number_of_components
        )
