# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import typing
from enum import Enum
from xml.dom import minidom

from kiara.utils.output import DictTabularWrap, TabularWrap

from kiara_modules.network_analysis.defaults import TableType
from kiara_modules.network_analysis.metadata_schemas import NetworkData


class NetworkDataTabularWrap(TabularWrap):
    def __init__(self, db: NetworkData, table_type: TableType):
        self._db: NetworkData = db
        self._table_type: TableType = table_type
        super().__init__()

    @property
    def _table_name(self):
        return self._table_type.value

    def retrieve_number_of_rows(self) -> int:

        from sqlalchemy import text

        with self._db.get_sqlalchemy_engine().connect() as con:
            result = con.execute(text(f"SELECT count(*) from {self._table_name}"))
            num_rows = result.fetchone()[0]

        return num_rows

    def retrieve_column_names(self) -> typing.Iterable[str]:

        from sqlalchemy import inspect

        engine = self._db.get_sqlalchemy_engine()
        inspector = inspect(engine)
        columns = inspector.get_columns(self._table_type.value)
        result = [column["name"] for column in columns]
        return result

    def slice(
        self, offset: int = 0, length: typing.Optional[int] = None
    ) -> "TabularWrap":

        from sqlalchemy import text

        query = f"SELECT * FROM {self._table_name}"
        if length:
            query = f"{query} LIMIT {length}"
        else:
            query = f"{query} LIMIT {self.num_rows}"
        if offset > 0:
            query = f"{query} OFFSET {offset}"
        with self._db.get_sqlalchemy_engine().connect() as con:
            result = con.execute(text(query))
            result_dict: typing.Dict[str, typing.List[typing.Any]] = {}
            for cn in self.column_names:
                result_dict[cn] = []
            for r in result:
                for i, cn in enumerate(self.column_names):
                    result_dict[cn].append(r[i])

        return DictTabularWrap(result_dict)

    def to_pydict(self) -> typing.Mapping:

        from sqlalchemy import text

        query = f"SELECT * FROM {self._table_name}"

        with self._db.get_sqlalchemy_engine().connect() as con:
            result = con.execute(text(query))
            result_dict: typing.Dict[str, typing.List[typing.Any]] = {}
            for cn in self.column_names:
                result_dict[cn] = []
            for r in result:
                for i, cn in enumerate(self.column_names):
                    result_dict[cn].append(r[i])

        return result_dict


class GraphTypesEnum(Enum):

    undirected = "undirected"
    directed = "directed"
    multi_directed = "multi_directed"
    multi_undirected = "multi_undirected"


def convert_graphml_type_to_sqlite(data_type: str) -> str:

    type_map = {
        "boolean": "INTEGER",
        "int": "INTEGER",
        "long": "INTEGER",
        "float": "REAL",
        "double": "REAL",
        "string": "TEXT",
    }

    return type_map[data_type]


def parse_graphml_file(path):
    """Adapted from the pygrahml Python library.

    Authors:
      - Hadrien Mary hadrien.mary@gmail.com
      - Nick Hamilton n.hamilton@imb.uq.edu.au

    Copyright (c) 2011, Hadrien Mary
    License: BSD 3-Clause

    """

    g = None
    with open(path, "r") as f:
        dom = minidom.parse(f)
        root = dom.getElementsByTagName("graphml")[0]
        graph = root.getElementsByTagName("graph")[0]
        name = graph.getAttribute("id")

        from pygraphml import Graph

        g = Graph(name)

        # Get attributes
        edge_map = {}
        node_map = {}

        edge_props = {}
        node_props = {}
        for attr in root.getElementsByTagName("key"):
            n_id = attr.getAttribute("id")
            name = attr.getAttribute("attr.name")
            for_type = attr.getAttribute("for")
            attr_type = attr.getAttribute("attr.type")
            if for_type == "edge":
                edge_map[n_id] = name
                edge_props[name] = {"type": convert_graphml_type_to_sqlite(attr_type)}
            else:
                node_map[n_id] = name
                node_props[name] = {"type": convert_graphml_type_to_sqlite(attr_type)}

        node_props_sorted = {}
        for key in sorted(node_map.keys()):
            node_props_sorted[node_map[key]] = node_props[node_map[key]]
        edge_props_sorted = {}
        for key in sorted(edge_map.keys()):
            edge_props_sorted[edge_map[key]] = edge_props[edge_map[key]]

        # Get nodes
        for node in graph.getElementsByTagName("node"):
            n = g.add_node(id=node.getAttribute("id"))

            for attr in node.getElementsByTagName("data"):
                key = attr.getAttribute("key")
                mapped = node_map[key]
                if attr.firstChild:
                    n[mapped] = attr.firstChild.data
                else:
                    n[mapped] = ""

        # Get edges
        for edge in graph.getElementsByTagName("edge"):
            source = edge.getAttribute("source")
            dest = edge.getAttribute("target")

            # source/target attributes refer to IDs: http://graphml.graphdrawing.org/xmlns/1.1/graphml-structure.xsd
            e = g.add_edge_by_id(source, dest)

            for attr in edge.getElementsByTagName("data"):
                key = attr.getAttribute("key")
                mapped = edge_map[key]
                if attr.firstChild:
                    e[mapped] = attr.firstChild.data
                else:
                    e[mapped] = ""

    return (g, edge_props_sorted, node_props_sorted)
