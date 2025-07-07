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

        # Get non-computed node columns (excluding internal columns starting with '_')
        nodes_table_columns = [LABEL_COLUMN_NAME]
        for column_name in nodes_table.column_names:
            if column_name.startswith("_"):
                continue
            nodes_table_columns.append(column_name)

        # Filter nodes by component and add a new sequential index
        nodes_query = f"""
            SELECT
                ROW_NUMBER() OVER (ORDER BY {NODE_ID_COLUMN_NAME}) - 1 AS {NODE_ID_COLUMN_NAME},
                {NODE_ID_COLUMN_NAME} AS old_node_id,
                {", ".join(nodes_table_columns)}  -- exclude NODE_ID_COLUMN_NAME since we're creating new_node_id
            FROM nodes_table
            WHERE {COMPONENT_ID_COLUMN_NAME} = {component_id}
            ORDER BY {NODE_ID_COLUMN_NAME}
        """
        nodes_result = duckdb.sql(nodes_query)

        # Create a mapping table for old_node_id -> new_node_id
        id_mapping_query = f"""
            SELECT old_node_id, {NODE_ID_COLUMN_NAME} FROM nodes_result
        """
        id_mapping_result = duckdb.sql(id_mapping_query)  # noqa

        # Get non-computed edge columns (excluding internal columns starting with '_')
        edges_table = network_data.edges.arrow_table
        edges_table_columns = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
        for column_name in edges_table.column_names:
            if column_name.startswith("_"):
                continue
            edges_table_columns.append(column_name)

        # Filter edges by component and translate node IDs using the mapping
        edges_query = f"""
            SELECT
                src_map.{NODE_ID_COLUMN_NAME} AS {SOURCE_COLUMN_NAME},
                e.{SOURCE_COLUMN_NAME} AS old_source_id,
                tgt_map.{NODE_ID_COLUMN_NAME} AS {TARGET_COLUMN_NAME},
                e.{TARGET_COLUMN_NAME} AS old_target_id
                {", " + ", ".join(edges_table_columns[2:]) if len(edges_table_columns) > 2 else ""}
            FROM edges_table e
            JOIN id_mapping_result src_map ON e.{SOURCE_COLUMN_NAME} = src_map.old_node_id
            JOIN id_mapping_result tgt_map ON e.{TARGET_COLUMN_NAME} = tgt_map.old_node_id
            WHERE e.{COMPONENT_ID_COLUMN_NAME} = {component_id}
        """
        edges_result = duckdb.sql(edges_query)

        network_data_result = NetworkData.create_network_data(
            nodes_table=nodes_result.arrow(), edges_table=edges_result.arrow()
        )

        return network_data_result
