# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_modules.network_analysis`` package.
"""
import typing

import networkx
import networkx as nx
from kiara import KiaraEntryPointItem
from kiara.data import Value
from kiara.data.types import ValueType
from kiara.utils.class_loading import find_value_types_under
from kiara_modules.core.metadata_schemas import KiaraDatabase

from kiara_modules.network_analysis.metadata_schemas import NetworkData
from kiara_modules.network_analysis.utils import NetworkDataTabularWrap, TableType

value_types: KiaraEntryPointItem = (
    find_value_types_under,
    ["kiara_modules.network_analysis.value_types"],
)


class NetworkGraphType(ValueType):
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


class NetworkDataType(ValueType):
    """Data that can be assembled into a graph.

    Internally, this is backed by a sqlite database, using https://github.com/dpapathanasiou/simple-graph .
    """

    _value_type_name = "network_data"

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [str, KiaraDatabase, NetworkData]

    def validate(cls, value: typing.Any) -> typing.Any:

        if isinstance(value, networkx.Graph):
            raise NotImplementedError()
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
