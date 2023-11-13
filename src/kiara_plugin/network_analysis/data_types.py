# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_plugin.network_analysis`` package.
"""
from typing import Any, ClassVar, List, Mapping, Type, Union

from rich.console import Group

from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.exceptions import KiaraException
from kiara.models.values.value import Value
from kiara.utils.output import ArrowTabularWrap
from kiara_plugin.network_analysis.defaults import (
    EDGES_TABLE_NAME,
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    NODES_TABLE_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.tabular.data_types.tables import TablesType
from kiara_plugin.tabular.models.tables import KiaraTables


class NetworkDataType(TablesType):
    """Data that can be assembled into a graph.

    This data type extends the 'database' type from the [kiara_plugin.tabular](https://github.com/DHARPA-Project/kiara_plugin.tabular) plugin, restricting the allowed tables to one called 'edges',
    and one called 'nodes'.
    """

    _data_type_name: ClassVar[str] = "network_data"

    @classmethod
    def python_class(cls) -> Type:
        return NetworkData  # type: ignore

    def parse_python_obj(self, data: Any) -> NetworkData:

        if isinstance(data, KiaraTables):
            if EDGES_TABLE_NAME not in data.tables.keys():
                raise KiaraException(
                    f"Can't import network data: no '{EDGES_TABLE_NAME}' table found"
                )

            if NODES_TABLE_NAME not in data.tables.keys():
                raise KiaraException(
                    f"Can't import network data: no '{NODES_TABLE_NAME}' table found"
                )

            # return NetworkData(
            #     tables={
            #         EDGES_TABLE_NAME: data.tables[EDGES_TABLE_NAME],
            #         NODES_TABLE_NAME: data.tables[NODES_TABLE_NAME],
            #     },
            #
            # )
            return NetworkData.create_network_data(
                edges_table=data.tables[EDGES_TABLE_NAME].arrow_table,
                nodes_table=data.tables[NODES_TABLE_NAME].arrow_table,
                augment_tables=False,
            )

        if not isinstance(data, NetworkData):
            raise KiaraException(
                f"Can't parse object to network data: invalid type '{type(data)}'."
            )

        return data

    def _validate(cls, value: Any) -> None:
        if not isinstance(value, NetworkData):
            raise ValueError(
                f"Invalid type '{type(value)}': must be of 'NetworkData' (or a sub-class)."
            )

        network_data: NetworkData = value

        table_names = network_data.table_names
        if EDGES_TABLE_NAME not in table_names:
            raise Exception(
                f"Invalid 'network_data' value: database does not contain table '{EDGES_TABLE_NAME}'."
            )
        if NODES_TABLE_NAME not in table_names:
            raise Exception(
                f"Invalid 'network_data' value: database does not contain table '{NODES_TABLE_NAME}'."
            )

        edges_columns = network_data.edges.column_names
        if SOURCE_COLUMN_NAME not in edges_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{SOURCE_COLUMN_NAME}' column. Available columns: {', '.join(edges_columns)}."
            )
        if TARGET_COLUMN_NAME not in edges_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{TARGET_COLUMN_NAME}' column. Available columns: {', '.join(edges_columns)}."
            )

        nodes_columns = network_data.nodes.column_names
        if NODE_ID_COLUMN_NAME not in nodes_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'nodes' table does not contain a '{NODE_ID_COLUMN_NAME}' column. Available columns: {', '.join(nodes_columns)}."
            )
        if LABEL_COLUMN_NAME not in nodes_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'nodes' table does not contain a '{LABEL_COLUMN_NAME}' column. Available columns: {', '.join(nodes_columns)}."
            )

    def pretty_print_as__terminal_renderable(
        self, value: Value, render_config: Mapping[str, Any]
    ) -> Any:

        max_rows = render_config.get(
            "max_no_rows", DEFAULT_PRETTY_PRINT_CONFIG["max_no_rows"]
        )
        max_row_height = render_config.get(
            "max_row_height", DEFAULT_PRETTY_PRINT_CONFIG["max_row_height"]
        )
        max_cell_length = render_config.get(
            "max_cell_length", DEFAULT_PRETTY_PRINT_CONFIG["max_cell_length"]
        )

        half_lines: Union[int, None] = None
        if max_rows:
            half_lines = int(max_rows / 2)

        network_data: NetworkData = value.data

        result: List[Any] = [""]

        nodes_atw = ArrowTabularWrap(network_data.nodes.arrow_table)
        nodes_pretty = nodes_atw.as_terminal_renderable(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        result.append(f"[b]{NODES_TABLE_NAME}[/b]")
        result.append(nodes_pretty)

        edges_atw = ArrowTabularWrap(network_data.edges.arrow_table)
        edges_pretty = edges_atw.as_terminal_renderable(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        result.append(f"[b]{EDGES_TABLE_NAME}[/b]")
        result.append(edges_pretty)

        return Group(*result)
