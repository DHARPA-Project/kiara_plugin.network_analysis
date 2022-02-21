# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_modules.network_analysis`` package.
"""
import typing

import networkx
import networkx as nx
from kiara import KiaraEntryPointItem
from kiara.data import Value
from kiara.data.types import ValueType
from kiara.data.types.core import AnyType
from kiara.utils.class_loading import find_value_types_under
from kiara_modules.core.metadata_models import KiaraDatabase
from kiara_modules.core.value_types import DatabaseType

from kiara_modules.network_analysis.defaults import (
    ID_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    TableType,
)
from kiara_modules.network_analysis.metadata_models import NetworkData
from kiara_modules.network_analysis.utils import NetworkDataTabularWrap

value_types: KiaraEntryPointItem = (
    find_value_types_under,
    ["kiara_modules.network_analysis.value_types"],
)


class NetworkGraphType(AnyType):
    """A network graph object.

    Internally, this is backed by a ``Graph`` object of the [networkx](https://networkx.org/) Python library.
    """

    _value_type_name = "network_graph"

    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional["ValueType"]:

        if isinstance(data, nx.Graph):
            return NetworkGraphType()
        else:
            return None

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [nx.Graph]

    def validate(cls, value: typing.Any) -> typing.Any:

        if not isinstance(value, networkx.Graph):
            raise ValueError(f"Invalid type '{type(value)}' for graph: {value}")
        return value


class NetworkDataType(DatabaseType):
    """Data that can be assembled into a graph.

    Internally, this is backed by a sqlite database, using https://github.com/dpapathanasiou/simple-graph .
    """

    _value_type_name = "network_data"

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [str, KiaraDatabase, NetworkData]

    def validate(cls, value: typing.Any) -> typing.Any:

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

        schema = network_data.get_schema_for_table("edges")
        if SOURCE_COLUMN_NAME not in schema.keys():
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{SOURCE_COLUMN_NAME}' column. Available columns: {', '.join(schema.keys())}."
            )
        if TARGET_COLUMN_NAME not in schema.keys():
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{TARGET_COLUMN_NAME}' column. Available columns: {', '.join(schema.keys())}."
            )

        schema = network_data.get_schema_for_table("nodes")
        if ID_COLUMN_NAME not in schema.keys():
            raise Exception(
                f"Invalid 'network_data' value: 'nodes' table does not contain a '{ID_COLUMN_NAME}' column. Available columns: {', '.join(schema.keys())}."
            )

        return value

    def pretty_print_as_renderables(
        self, value: Value, print_config: typing.Mapping[str, typing.Any]
    ) -> typing.Any:

        max_rows = print_config.get("max_no_rows")
        max_row_height = print_config.get("max_row_height")
        max_cell_length = print_config.get("max_cell_length")

        half_lines: typing.Optional[int] = None
        if max_rows:
            half_lines = int(max_rows / 2)

        db: NetworkData = value.get_value_data()

        result: typing.List[typing.Any] = [""]
        atw = NetworkDataTabularWrap(db=db, table_type=TableType.EDGES)
        pretty = atw.pretty_print(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        result.append(f"[b]Table[/b]: [i]{TableType.EDGES.value}[/i]")
        result.append(pretty)

        atw = NetworkDataTabularWrap(db=db, table_type=TableType.NODES)
        pretty = atw.pretty_print(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        result.append(f"[b]Table[/b]: [i]{TableType.NODES.value}[/i]")
        result.append(pretty)
        return result
