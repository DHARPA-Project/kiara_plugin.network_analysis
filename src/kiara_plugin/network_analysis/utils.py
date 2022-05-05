# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import typing

from kiara.utils.output import DictTabularWrap, TabularWrap

from kiara_plugin.network_analysis.defaults import (
    DEFAULT_NETWORK_DATA_CHUNK_SIZE,
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    NetworkDataTableType,
)

if typing.TYPE_CHECKING:
    import networkx as nx
    import pyarrow as pa
    from sqlalchemy import MetaData, Table  # noqa

    from kiara_plugin.network_analysis.models import NetworkData


class NetworkDataTabularWrap(TabularWrap):
    def __init__(self, db: "NetworkData", table_type: NetworkDataTableType):
        self._db: NetworkData = db
        self._table_type: NetworkDataTableType = table_type
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


def insert_table_data_into_network_graph(
    network_data: "NetworkData",
    edges_table: "pa.Table",
    edges_column_map: typing.Optional[typing.Mapping[str, str]] = None,
    nodes_table: typing.Optional["pa.Table"] = None,
    nodes_column_map: typing.Optional[typing.Mapping[str, str]] = None,
    chunk_size: int = DEFAULT_NETWORK_DATA_CHUNK_SIZE,
):

    added_node_ids = set()

    if edges_column_map is None:
        edges_column_map = {}
    if nodes_column_map is None:
        nodes_column_map = {}

    if nodes_table is not None:
        for batch in nodes_table.to_batches(chunk_size):
            batch_dict = batch.to_pydict()

            if nodes_column_map:
                for k, v in nodes_column_map.items():
                    if k in batch_dict.keys():
                        if k == ID_COLUMN_NAME and v == LABEL_COLUMN_NAME:
                            _data = batch_dict.get(k)
                        else:
                            _data = batch_dict.pop(k)
                            if v in batch_dict.keys():
                                raise Exception(
                                    "Duplicate nodes column name after mapping: {v}"
                                )
                        batch_dict[v] = _data
            if LABEL_COLUMN_NAME not in batch_dict.keys():
                batch_dict[LABEL_COLUMN_NAME] = (
                    str(x) for x in batch_dict[ID_COLUMN_NAME]
                )

            ids = batch_dict[ID_COLUMN_NAME]
            data = [dict(zip(batch_dict, t)) for t in zip(*batch_dict.values())]
            network_data.insert_nodes(*data)

            added_node_ids.update(ids)

    for batch in edges_table.to_batches(chunk_size):

        batch_dict = batch.to_pydict()

        for k, v in edges_column_map.items():
            if k in batch_dict.keys():
                _data = batch_dict.pop(k)
                if v in batch_dict.keys():
                    raise Exception("Duplicate edges column name after mapping: {v}")
                batch_dict[v] = _data

        data = [dict(zip(batch_dict, t)) for t in zip(*batch_dict.values())]

        all_node_ids = network_data.insert_edges(
            *data,
            existing_node_ids=added_node_ids,
        )
        added_node_ids.update(all_node_ids)


def extract_edges_as_table(graph: "nx.Graph"):

    # adapted from networx code
    # License: 3-clause BSD license
    # Copyright (C) 2004-2022, NetworkX Developers

    import networkx as nx
    import pyarrow as pa

    edgelist = graph.edges(data=True)
    source_nodes = [s for s, _, _ in edgelist]
    target_nodes = [t for _, t, _ in edgelist]

    all_attrs: typing.Set[str] = set().union(*(d.keys() for _, _, d in edgelist))  # type: ignore

    if SOURCE_COLUMN_NAME in all_attrs:
        raise nx.NetworkXError(
            f"Source name {SOURCE_COLUMN_NAME} is an edge attribute name"
        )
    if SOURCE_COLUMN_NAME in all_attrs:
        raise nx.NetworkXError(
            f"Target name {SOURCE_COLUMN_NAME} is an edge attribute name"
        )

    nan = float("nan")
    edge_attr = {k: [d.get(k, nan) for _, _, d in edgelist] for k in all_attrs}

    edge_lists = {
        SOURCE_COLUMN_NAME: source_nodes,
        TARGET_COLUMN_NAME: target_nodes,
    }

    edge_lists.update(edge_attr)
    edges_table = pa.Table.from_pydict(mapping=edge_lists)

    return edges_table


def extract_nodes_as_table(graph: "nx.Graph"):

    # adapted from networx code
    # License: 3-clause BSD license
    # Copyright (C) 2004-2022, NetworkX Developers

    import networkx as nx
    import pyarrow as pa

    nodelist = graph.nodes(data=True)

    node_ids = [n for n, _ in nodelist]

    all_attrs: typing.Set[str] = set().union(*(d.keys() for _, d in nodelist))  # type: ignore

    if ID_COLUMN_NAME in all_attrs:
        raise nx.NetworkXError(
            f"Id column name {ID_COLUMN_NAME} is an node attribute name"
        )
    if SOURCE_COLUMN_NAME in all_attrs:
        raise nx.NetworkXError(
            f"Target name {SOURCE_COLUMN_NAME} is an edge attribute name"
        )

    nan = float("nan")
    node_attr = {k: [d.get(k, nan) for _, d in nodelist] for k in all_attrs}

    node_attr[ID_COLUMN_NAME] = node_ids
    nodes_table = pa.Table.from_pydict(mapping=node_attr)

    return nodes_table
