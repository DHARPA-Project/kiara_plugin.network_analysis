# -*- coding: utf-8 -*-
from typing import Any, Dict, Mapping, Union

from kiara.models.values.value import Value
from kiara.modules import ValueMapSchema
from kiara.modules.included_core_modules.filter import FilterModule
from kiara_plugin.network_analysis import NetworkData
from kiara_plugin.network_analysis.defaults import (
    COMPONENT_ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)


class NetworkDataFiltersModule(FilterModule):
    _module_type_name = "network_data.filters"

    @classmethod
    def retrieve_supported_type(cls) -> Union[Dict[str, Any], str]:
        return "network_data"

    def create_filter_inputs(self, filter_name: str) -> Union[None, ValueMapSchema]:
        if filter_name == "select_component":
            return {
                "component_id": {
                    "type": "integer",
                    "doc": "The id of the componen to select.",
                    "optional": False,
                    "default": 0,
                },
            }
        return None

    def filter__select_component(self, value: Value, filter_inputs: Mapping[str, Any]):
        import duckdb

        component_id = filter_inputs["component_id"]

        network_data: NetworkData = value.data

        nodes_table = network_data.nodes.arrow_table

        nodes_table_columns = [NODE_ID_COLUMN_NAME, LABEL_COLUMN_NAME]
        for column_name in nodes_table.column_names:
            if column_name.startswith("_"):
                continue
            nodes_table_columns.append(column_name)

        nodes_query = f"""
            SELECT {", ".join(nodes_table_columns)} FROM nodes_table WHERE {COMPONENT_ID_COLUMN_NAME} = {component_id}
        """
        nodes_result = duckdb.sql(nodes_query)
        dbg(nodes_result)

        edges_table = network_data.edges.arrow_table
        edges_table_columns = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
        for column_name in edges_table.column_names:
            if column_name.startswith("_"):
                continue
            edges_table_columns.append(column_name)

        edges_query = f"""
            SELECT {", ".join(edges_table_columns)} FROM edges_table WHERE {COMPONENT_ID_COLUMN_NAME} = {component_id}
        """
        edges_result = duckdb.sql(edges_query)
        network_data_result = NetworkData.create_network_data(
            nodes_table=nodes_result.arrow(), edges_table=edges_result.arrow()
        )
        dbg(network_data_result)
        return network_data_result
