# -*- coding: utf-8 -*-
from typing import Any, Dict, List, Mapping, Union

from kiara.api import KiaraModule, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
from kiara_plugin.core_types.data_types.models import KiaraModelList
from kiara_plugin.network_analysis.defaults import (
    ALLOWED_AGGREGATION_FUNCTIONS,
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
    "tags": ["network", "network analysis", "network_graphs"],
}


def generate_redefine_edges_doc():
    REDEFINE_EDGES_DOC = """Redefine edges by merging duplicate edges and applying aggregation functions to certain edge attributes.

The main use cases for this::

  - 'flatten' a multigraph into a non-multigraph one
  - independently aggregate existing edge attribute columns into the new network_data instance
  - let the user specify columns that will be included in the 'group_by' clause of the generated sql query (in which case we don't create a not-multigraph, but a multigraph with a different set of parallel edges)
  - rename existing edge attributes

By default, this operation does not copy existing any attributes, so every edge attribute that should be contained in the resulting, new network_data instance needs to be specified in the 'columns' input.
Automatically computed columns (those that start with '_') can be used as source columns, but need to be renamed to not start with "_".

If no target_column is specified, the original source column name is used. If no transform function is specified, the column data is copied as is.

### Available transformation functions:

"""

    funcs_doc = ""
    for name, doc in ALLOWED_AGGREGATION_FUNCTIONS.items():
        funcs_doc = f"{funcs_doc}\n - ***{name}***: {doc}"

    doc = f"{REDEFINE_EDGES_DOC}\n\n{funcs_doc}"

    EXAMPLES = """### Examples

In the commandline, *kiara* will parse each input string as a column transformation. If the string does not contain a '=', the column will not be renamed, and the default transformation will be applied (COUNT).

```
kiara run network_data.redefine_edges network_data=simple columns=weight
```

If you want to use a transformation or rename an attribute, you can specify the details after a '=':

```
kiara run network_data.redefine_edges network_data=simple 'columns=new_time_column_name=time' 'columns=sum_weight=SUM(weight)'
```

This example would copy all contents of the original 'time' column to a new column named 'new_time_column_name' (with a list of all 'time' values of a specific source/target combination), and create a column 'sum_weight' that contains the sum of all 'weight' values of duplicate edges.

If using this operation from Python or some other way, the 'column' inputs for the previous two examples would look like:

```
inputs:
  columns:
    - target_column_name: weight
      source_column_name: weight
      transform_function: sum
    - target_column_name: time
      source_column_name: new_time_column_name
      transform_function: list
```

and

```
inputs:
  columns:
    - target_column_name: new_time_column_name
      source_column_name: time
      transform_function: list
    - target_column_name: count_weight
      source_column_name: weight
      transform_function: count
```
"""

    doc = f"{doc}\n\n{EXAMPLES}"
    return doc


class RedefineNetworkEdgesModule(KiaraModule):
    """Redefine edges by merging duplicate edges and applying aggregation functions to certain edge attributes."""

    _module_type_name = "network_data.redefine_edges"

    KIARA_METADATA = {
        "references": {
            "discussion": {
                "url": "https://github.com/DHARPA-Project/kiara_plugin.network_analysis/discussions/23"
            }
        }
    }

    @classmethod
    def type_doc(cls):

        return generate_redefine_edges_doc()

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:
        result: Mapping[str, Mapping[str, Any]] = {
            "network_data": {
                "type": "network_data",
                "doc": "The network data to flatten.",
            },
            "columns": {
                "type": "kiara_model_list",
                "type_config": {
                    "kiara_model_id": "input.network_analysis_attribute_map_transformation"
                },
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

        network_data_obj = inputs.get_value_obj("network_data")
        network_data: NetworkData = network_data_obj.data

        # needed for sql query later
        edges_table = network_data.edges.arrow_table  # noqa

        attr_map_strategies: Union[
            None, KiaraModelList[AttributeMapStrategy]
        ] = inputs.get_value_data("columns")

        if attr_map_strategies:

            invalid_columns = set()
            for strategy in attr_map_strategies.list_items:

                # if strategy.source_column_name == SOURCE_COLUMN_NAME:
                #     raise KiaraProcessingException(
                #         msg=f"Can't redefine edges with provided attribute map: the source column name '{SOURCE_COLUMN_NAME}' is reserved."
                #     )

                if strategy.source_column_name == TARGET_COLUMN_NAME:
                    raise KiaraProcessingException(
                        msg=f"Can't redefine edges with provided attribute map: the target column name '{TARGET_COLUMN_NAME}' is reserved."
                    )

                if strategy.target_column_name.startswith("_"):
                    raise KiaraProcessingException(
                        msg=f"Can't redefine edges with provided column map: the target column name '{strategy.target_column_name}' starts with an underscore, which is reserved for automatically computed edge attributes."
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
                    # column_type = edges_table.field(strategy.source_column_name).type
                    # if pa.types.is_integer(column_type) or pa.types.is_floating(
                    #     column_type
                    # ):
                    #     transform_function = "SUM"
                    # else:
                    #     transform_function = "LIST"
                    transform_function = "COUNT"
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
