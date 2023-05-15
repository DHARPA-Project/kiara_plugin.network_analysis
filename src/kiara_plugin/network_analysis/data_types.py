# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_plugin.network_analysis`` package.
"""
from typing import Any, List, Mapping, Type, Union

from rich.console import Group

from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.models.values.value import Value
from kiara_plugin.network_analysis.defaults import (
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    RANKING_COLUNN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    NetworkDataTableType,
)
from kiara_plugin.network_analysis.models import GraphRankingData, NetworkData
from kiara_plugin.tabular.data_types.db import DatabaseType, SqliteTabularWrap
from kiara_plugin.tabular.models.db import KiaraDatabase


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
        if NetworkDataTableType.EDGES.value not in table_names:
            raise Exception(
                f"Invalid 'network_data' value: database does not contain table '{NetworkDataTableType.EDGES.value}'"
            )
        if "nodes" not in table_names:
            raise Exception(
                f"Invalid 'network_data' value: database does not contain table '{NetworkDataTableType.NODES.value}'"
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


class GraphRankingDataType(DatabaseType):
    """A database (table) containing ranking data for a network graph."""

    _data_type_name = "graph_ranking_data"

    @classmethod
    def python_class(cls) -> Type:
        return GraphRankingData

    def parse_python_obj(self, data: Any) -> GraphRankingData:

        if isinstance(data, str):
            # TODO: check path exists
            return GraphRankingData.create_graph_ranking_data_from_sqlite(
                db_file_path=data
            )
        elif isinstance(data, KiaraDatabase):
            return GraphRankingData.create_graph_ranking_data_from_database(db=data)

        return data

    def _validate(cls, value: Any) -> None:

        if not isinstance(value, GraphRankingData):
            raise ValueError(
                f"Invalid type '{type(value)}': must be of 'GraphRankingData' (or a sub-class)."
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

        db: KiaraDatabase = value.data

        result: List[Any] = [""]
        for table_name in db.table_names:
            atw = SqliteTabularWrap(
                engine=db.get_sqlalchemy_engine(),
                table_name=table_name,
                sort_column_names=[RANKING_COLUNN_NAME],
            )
            pretty = atw.as_terminal_renderable(
                rows_head=half_lines,
                rows_tail=half_lines,
                max_row_height=max_row_height,
                max_cell_length=max_cell_length,
            )
            result.append(f"[b]Table[/b]: [i]{table_name}[/i]")
            result.append(pretty)

        return Group(*result)
