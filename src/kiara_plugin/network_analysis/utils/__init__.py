# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Hashable,
    Iterable,
    List,
    Mapping,
    Tuple,
    Union,
)

from kiara.exceptions import KiaraException
from kiara.utils.output import DictTabularWrap, TabularWrap
from kiara_plugin.network_analysis.defaults import (
    DEFAULT_NETWORK_DATA_CHUNK_SIZE,
    NODE_ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    WEIGHT_DIRECTED_COLUMN_NAME,
    WEIGHT_UNDIRECTED_COLUMN_NAME,
    NetworkDataTableType,
)

if TYPE_CHECKING:
    import networkx as nx
    import polars as pl
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

    def retrieve_column_names(self) -> Iterable[str]:
        from sqlalchemy import inspect

        engine = self._db.get_sqlalchemy_engine()
        inspector = inspect(engine)
        columns = inspector.get_columns(self._table_type.value)
        result = [column["name"] for column in columns]
        return result

    def slice(self, offset: int = 0, length: Union[int, None] = None) -> "TabularWrap":
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
            result_dict: Dict[str, List[Any]] = {}
            for cn in self.column_names:
                result_dict[cn] = []
            for r in result:
                for i, cn in enumerate(self.column_names):
                    result_dict[cn].append(r[i])

        return DictTabularWrap(result_dict)

    def to_pydict(self) -> Mapping:
        from sqlalchemy import text

        query = f"SELECT * FROM {self._table_name}"

        with self._db.get_sqlalchemy_engine().connect() as con:
            result = con.execute(text(query))
            result_dict: Dict[str, List[Any]] = {}
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
    edges_column_map: Union[Mapping[str, str], None] = None,
    nodes_table: Union["pa.Table", None] = None,
    nodes_column_map: Union[Mapping[str, str], None] = None,
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
                        if k == NODE_ID_COLUMN_NAME and v == LABEL_COLUMN_NAME:
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
                    str(x) for x in batch_dict[NODE_ID_COLUMN_NAME]
                )

            ids = batch_dict[NODE_ID_COLUMN_NAME]
            data = [dict(zip(batch_dict, t)) for t in zip(*batch_dict.values())]
            network_data.insert_nodes(*data)

            added_node_ids.update(ids)
    else:
        raise KiaraException("Nodes table is required to create network data.")

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


def extract_edges_as_table(
    graph: "nx.Graph", node_id_map: Dict[Hashable, int]
) -> "pa.Table":
    """Extract the edges of this graph as a pyarrow table.

    The provided `node_id_map` might be modified if a node id is not yet in the map.

    Args:
        graph: The graph to extract edges from.
        node_id_map: A mapping from (original) node ids to (kiara-internal) (integer) node-ids.
    """

    # adapted from networx code
    # License: 3-clause BSD license
    # Copyright (C) 2004-2022, NetworkX Developers

    import pyarrow as pa

    if node_id_map is None:
        node_id_map = {}

    # nan = float("nan")

    max_node_id = max(node_id_map.values())  # TODO: could we just use len(node_id_map)?
    edge_columns: Dict[str, List[int]] = {
        SOURCE_COLUMN_NAME: [],
        TARGET_COLUMN_NAME: [],
    }

    for source, target, edge_data in graph.edges(data=True):
        if source not in node_id_map.keys():
            max_node_id += 1
            node_id_map[source] = max_node_id
        if target not in node_id_map.keys():
            max_node_id += 1
            node_id_map[target] = max_node_id

        edge_columns[SOURCE_COLUMN_NAME].append(node_id_map[source])
        edge_columns[TARGET_COLUMN_NAME].append(node_id_map[target])

        for k in edge_data.keys():
            if k.startswith("_"):
                raise KiaraException(
                    "Graph contains edge column name starting with '_'. This is reserved for internal use, and not allowed."
                )

            v = edge_data.get(k, None)
            edge_columns.setdefault(k, []).append(v)

    edges_table = pa.Table.from_pydict(mapping=edge_columns)

    return edges_table


def extract_nodes_as_table(
    graph: "nx.Graph",
    label_attr_name: Union[str, None, Iterable[str]] = None,
    ignore_attributes: Union[None, Iterable[str]] = None,
) -> Tuple["pa.Table", Dict[Hashable, int]]:
    # adapted from networx code
    # License: 3-clause BSD license
    # Copyright (C) 2004-2022, NetworkX Developers

    import pyarrow as pa

    # nan = float("nan")

    nodes: Dict[str, List[Any]] = {
        NODE_ID_COLUMN_NAME: [],
        LABEL_COLUMN_NAME: [],
    }
    nodes_map = {}

    for i, (node_id, node_data) in enumerate(graph.nodes(data=True)):
        nodes[NODE_ID_COLUMN_NAME].append(i)
        if label_attr_name is None:
            nodes[LABEL_COLUMN_NAME].append(str(node_id))
        elif isinstance(label_attr_name, str):
            label = node_data.get(label_attr_name, None)
            if label:
                nodes[LABEL_COLUMN_NAME].append(str(label))
            else:
                nodes[LABEL_COLUMN_NAME].append(str(node_id))
        else:
            label_final = None
            for label in label_attr_name:
                label_final = node_data.get(label, None)
                if label_final:
                    break
            if not label_final:
                label_final = node_id
            nodes[LABEL_COLUMN_NAME].append(str(label_final))

        nodes_map[node_id] = i
        for k in node_data.keys():
            if ignore_attributes and k in ignore_attributes:
                continue

            if k.startswith("_"):
                raise KiaraException(
                    "Graph contains node column name starting with '_'. This is reserved for internal use, and not allowed."
                )

            v = node_data.get(k, None)
            nodes.setdefault(k, []).append(v)

    nodes_table = pa.Table.from_pydict(mapping=nodes)

    return nodes_table, nodes_map


def augment_edges_table(edges_table: Union["pa.Table", "pl.DataFrame"]) -> "pa.Table":

    import duckdb

    try:
        column_names = edges_table.column_names  # type: ignore
    except Exception:
        column_names = edges_table.columns  # type: ignore

    edge_attr_columns = [x for x in column_names if not x.startswith("_")]
    if edge_attr_columns:
        other_columns = ", " + ", ".join(edge_attr_columns)
    else:
        other_columns = ""

    query = f"SELECT {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME}, COUNT(*) OVER (PARTITION BY {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME}) as {WEIGHT_DIRECTED_COLUMN_NAME}, COUNT(*) OVER (PARTITION BY LEAST({SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME}), GREATEST({SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME})) as {WEIGHT_UNDIRECTED_COLUMN_NAME} {other_columns} FROM edges_table"

    result = duckdb.sql(query)
    edges_table_augmented = result.arrow()
    return edges_table_augmented


def augment_nodes_table(
    nodes_table: Union["pa.Table", "pl.DataFrame"], augmented_edges_table: "pa.Table"
) -> "pa.Table":
    import duckdb

    try:
        column_names = nodes_table.column_names  # type: ignore
    except Exception:
        column_names = nodes_table.columns  # type: ignore

    node_attr_columns = [x for x in column_names if not x.startswith("_")]
    if node_attr_columns:
        ", " + ", ".join(node_attr_columns)
    else:
        pass

    query = """
        select
            NT._id,
            COALESCE(COUNT(ET_S._source), 0) as count_source,
            COALESCE(COUNT(ET_T._target), 0) as count_target
        from nodes_table NT
        LEFT JOIN augmented_edges_table ET_S
        ON NT._id = ET_S._source
        LEFT JOIN augmented_edges_table ET_T
        ON NT._id = ET_T._target
        GROUP BY NT._id
    """
    result = duckdb.sql(query)
    nodes_table_augmented = result.arrow()
    dbg(result)
    dbg(augmented_edges_table)
    import sys

    sys.exit()
    return nodes_table_augmented
