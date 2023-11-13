# -*- coding: utf-8 -*-
from typing import Any, Dict, List, Mapping, Union

from kiara.api import KiaraModule, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
from kiara_plugin.core_types.data_types.models import KiaraModelList
from kiara_plugin.network_analysis.defaults import (
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.network_analysis.models.inputs import AttributeMapStrategy

KIARA_METADATA = {
    "authors": [
        {"name": "Lena Jaskov", "email": "helena.jaskov@uni.lu"},
        {"name": "Markus Binsteiner", "email": "markus@frkl.io"},
    ],
    "description": "Modules related to extracting components from network data.",
}


class RedefineNetworkEdgesModule(KiaraModule):
    """Redefine edges by merging duplicate edges and applying aggregation functions to certain edge attributes."""

    _module_type_name = "network_data.redefine_edges"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:
        result: Mapping[str, Mapping[str, Any]] = {
            "network_data": {
                "type": "network_data",
                "doc": "The network data to flatten.",
            },
            "attribute_map_strategies": {
                "type": "kiara_model_list",
                "type_config": {"kiara_model_id": "input.attribute_map_strategy"},
                "doc": "A list of specs on how to map existing attributes onto the target network edge data.",
                "optional": True,
            },
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

        return result

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import duckdb
        import pyarrow as pa

        network_data_obj = inputs.get_value_obj("network_data")
        network_data: NetworkData = network_data_obj.data

        edges_table = network_data.edges.arrow_table

        attr_map_strategies: Union[
            None, KiaraModelList[AttributeMapStrategy]
        ] = inputs.get_value_data("attribute_map_strategies")

        if attr_map_strategies:

            invalid_columns = set()
            for strategy in attr_map_strategies.list_items:

                if strategy.source_column_name == SOURCE_COLUMN_NAME:
                    raise KiaraProcessingException(
                        msg=f"Can't redefine edges with provided attribute map: the source column name '{SOURCE_COLUMN_NAME}' is reserved."
                    )

                if strategy.source_column_name == TARGET_COLUMN_NAME:
                    raise KiaraProcessingException(
                        msg=f"Can't redefine edges with provided attribute map: the target column name '{TARGET_COLUMN_NAME}' is reserved."
                    )

                if strategy.source_column_name not in network_data.edges.column_names:
                    invalid_columns.add(strategy.source_column_name)

            if invalid_columns:

                msg = f"Can't redefine edges with provided attribute map strategies: the following columns are not available in the network data: {', '.join(invalid_columns)}"

                msg = f"{msg}\n\nAvailable column names:\n\n"
                for col_name in (
                    x for x in network_data.edges.column_names if not x.startswith("_")
                ):
                    msg = f"{msg}\n - {col_name}"
                raise KiaraProcessingException(msg=msg)

        sql_tokens: List[str] = []
        group_bys = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
        if attr_map_strategies:
            for strategy in attr_map_strategies.list_items:

                if not strategy.transform_function:
                    column_type = edges_table.field(strategy.source_column_name).type
                    if pa.types.is_integer(column_type) or pa.types.is_floating(
                        column_type
                    ):
                        transform_function = "SUM"
                    else:
                        transform_function = "LIST"
                else:
                    transform_function = strategy.transform_function

                transform_function = transform_function.lower()
                if transform_function == "group_by":
                    group_bys.append(strategy.source_column_name)
                    sql_token = None
                elif transform_function == "string_agg_comma":
                    sql_token = f"STRING_AGG({strategy.source_column_name}, ',') as {strategy.target_column_name}"
                else:
                    sql_token = f"{transform_function.upper()}({strategy.source_column_name}) as {strategy.target_column_name}"
                if sql_token:
                    sql_tokens.append(sql_token)

        query = f"""
        SELECT
            {', '.join(group_bys)},
            {', '.join(sql_tokens)}
        FROM edges_table
        GROUP BY {', '.join(group_bys)}
        """

        result = duckdb.sql(query)
        new_edges_table = result.arrow()
        network_data = NetworkData.create_network_data(
            nodes_table=network_data.nodes.arrow_table,
            edges_table=new_edges_table,
            augment_tables=True,
        )
        outputs.set_values(network_data=network_data)
