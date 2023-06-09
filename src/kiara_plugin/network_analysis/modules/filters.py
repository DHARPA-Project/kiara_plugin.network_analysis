# -*- coding: utf-8 -*-
from typing import Any, Dict, Mapping, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.values.value import Value
from kiara.modules import ValueMapSchema
from kiara.modules.included_core_modules.filter import FilterModule
from kiara_plugin.network_analysis.defaults import (
    COMPONENT_ID_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData


class TableFiltersModule(FilterModule):

    _module_type_name = "network_data.filters"

    @classmethod
    def retrieve_supported_type(cls) -> Union[Dict[str, Any], str]:

        return "network_data"

    def create_filter_inputs(self, filter_name: str) -> Union[None, ValueMapSchema]:

        if filter_name == "component":
            return {
                "component_id": {
                    "type": "string",
                    "doc": "The id of the component to extract.",
                    "default": "0",
                },
                "component_column": {
                    "type": "string",
                    "doc": "The name of the colum that contains the component id.",
                    "default": COMPONENT_ID_COLUMN_NAME,
                },
            }

        return None

    def filter__component(self, value: Value, filter_inputs: Mapping[str, Any]):
        """Retrieve a single sub-component from a network data object."""

        component_id = filter_inputs["component_id"]
        component_column = filter_inputs["component_column"]

        network_data: NetworkData = value.data

        if component_column not in network_data.nodes.column_names:
            msg = f"Component column `{component_column}` not valid for this network_data instance.\n\nAvailable column names:\n\n"

            for attr in network_data.nodes.column_names:
                msg += f"  - {attr}\n"

            if component_column == COMPONENT_ID_COLUMN_NAME:
                msg = f"{msg}\n\nTry to run the `network_data.extract_components` module on your network_data before using this module."

            raise KiaraProcessingException(msg)

        network_data.nodes.arrow_table.column(component_column).type
        # filter_item = pa.scalar(component_id, type=pa.int32())

        query = f"select {NODE_ID_COLUMN_NAME} from nodes where {component_column} = {component_id}"
        node_result = network_data.query_nodes(query)

        network_data = NetworkData.from_filtered_nodes(
            network_data=network_data,
            nodes_list=node_result.column(NODE_ID_COLUMN_NAME).to_pylist(),
        )

        return network_data
