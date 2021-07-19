# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_modules.network_analysis`` package.
"""
import typing

import networkx
import networkx as nx
from kiara import KiaraEntryPointItem
from kiara.data.types import ValueType
from kiara.utils.class_loading import find_value_types_under

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
