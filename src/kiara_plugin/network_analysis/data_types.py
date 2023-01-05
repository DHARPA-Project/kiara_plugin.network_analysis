# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_plugin.network_analysis`` package.
"""
from typing import Any, Type

from kiara_plugin.tabular.data_types.db import DatabaseType
from kiara_plugin.tabular.models.db import KiaraDatabase

from kiara_plugin.network_analysis.defaults import (
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData


class NetworkDataType(DatabaseType):
    """Data that can be assembled into a graph.

    This data type extends the 'database' type from the [kiara_plugin.tabular](https://github.com/DHARPA-Project/kiara_plugin.tabular) plugin, restricting the allowed tables to one called 'edges',
    and one called 'nodes'.
    """

    _data_type_name = "network_data"

    @classmethod
    def python_class(cls) -> Type:
        return NetworkData

    def parse_python_obj(self, data: Any) -> NetworkData:

        if isinstance(data, str):
            # TODO: check path exists
            return NetworkData.create_network_data_from_sqlite(db_file_path=data)
        elif isinstance(data, KiaraDatabase):
            return NetworkData.create_network_data_from_database(db=data)

        return data

    def _validate(cls, value: Any) -> None:

        if not isinstance(value, NetworkData):
            raise ValueError(
                f"Invalid type '{type(value)}': must be of 'NetworkData' (or a sub-class)."
            )

        network_data: NetworkData = value

        table_names = network_data.table_names
        for tn in ["edges", "nodes"]:
            if tn not in table_names:
                raise Exception(
                    f"Invalid 'network_data' value: database does not contain table '{tn}'"
                )

        table_names = network_data.table_names
        if "edges" not in table_names:
            raise Exception(
                "Invalid 'network_data' value: database does not contain table 'edges'"
            )
        if "nodes" not in table_names:
            raise Exception(
                "Invalid 'network_data' value: database does not contain table 'nodes'"
            )

        edges_columns = network_data.edges_schema.columns
        if SOURCE_COLUMN_NAME not in edges_columns.keys():
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{SOURCE_COLUMN_NAME}' column. Available columns: {', '.join(edges_columns.keys())}."
            )
        if TARGET_COLUMN_NAME not in edges_columns.keys():
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{TARGET_COLUMN_NAME}' column. Available columns: {', '.join(edges_columns.keys())}."
            )

        nodes_columns = network_data.nodes_schema.columns
        if ID_COLUMN_NAME not in nodes_columns.keys():
            raise Exception(
                f"Invalid 'network_data' value: 'nodes' table does not contain a '{ID_COLUMN_NAME}' column. Available columns: {', '.join(nodes_columns.keys())}."
            )
        if LABEL_COLUMN_NAME not in nodes_columns.keys():
            raise Exception(
                f"Invalid 'network_data' value: 'nodes' table does not contain a '{LABEL_COLUMN_NAME}' column. Available columns: {', '.join(nodes_columns.keys())}."
            )

    # def render_as__terminal_renderable(
    #     self, value: Value, render_config: Mapping[str, Any], manifest: Manifest
    # ) -> Any:
    #
    #     max_rows = render_config.get(
    #         "max_no_rows", DEFAULT_PRETTY_PRINT_CONFIG["max_no_rows"]
    #     )
    #     max_row_height = render_config.get(
    #         "max_row_height", DEFAULT_PRETTY_PRINT_CONFIG["max_row_height"]
    #     )
    #     max_cell_length = render_config.get(
    #         "max_cell_length", DEFAULT_PRETTY_PRINT_CONFIG["max_cell_length"]
    #     )
    #
    #     half_lines: Union[int, None] = None
    #     if max_rows:
    #         half_lines = int(max_rows / 2)
    #
    #     db: NetworkData = value.data
    #
    #     result: List[Any] = [""]
    #     atw = NetworkDataTabularWrap(db=db, table_type=NetworkDataTableType.EDGES)
    #     pretty = atw.as_terminal_renderable(
    #         rows_head=half_lines,
    #         rows_tail=half_lines,
    #         max_row_height=max_row_height,
    #         max_cell_length=max_cell_length,
    #     )
    #     result.append(f"[b]Table[/b]: [i]{NetworkDataTableType.EDGES.value}[/i]")
    #     result.append(pretty)
    #
    #     atw = NetworkDataTabularWrap(db=db, table_type=NetworkDataTableType.NODES)
    #     pretty = atw.as_terminal_renderable(
    #         rows_head=half_lines,
    #         rows_tail=half_lines,
    #         max_row_height=max_row_height,
    #         max_cell_length=max_cell_length,
    #     )
    #     result.append(f"[b]Table[/b]: [i]{NetworkDataTableType.NODES.value}[/i]")
    #     result.append(pretty)
    #     return Group(*result)
