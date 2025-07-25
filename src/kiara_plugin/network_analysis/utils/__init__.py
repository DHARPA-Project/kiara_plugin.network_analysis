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
    Tuple,
    Union,
)

import duckdb
from polars import DataFrame

from kiara.exceptions import KiaraException
from kiara_plugin.network_analysis.defaults import (
    COMPONENT_ID_COLUMN_NAME,
    CONNECTIONS_COLUMN_NAME,
    CONNECTIONS_MULTI_COLUMN_NAME,
    COUNT_DIRECTED_COLUMN_NAME,
    COUNT_IDX_DIRECTED_COLUMN_NAME,
    COUNT_IDX_UNDIRECTED_COLUMN_NAME,
    COUNT_UNDIRECTED_COLUMN_NAME,
    EDGE_ID_COLUMN_NAME,
    IN_DIRECTED_COLUMN_NAME,
    IN_DIRECTED_MULTI_COLUMN_NAME,
    LABEL_ALIAS_NAMES,
    LABEL_COLUMN_NAME,
    NODE_ID_ALIAS_NAMES,
    NODE_ID_COLUMN_NAME,
    NODE_IS_SOURCE_COLUMN_NAME,
    NODE_IS_TARGET_COLUMN_NAME,
    OUT_DIRECTED_COLUMN_NAME,
    OUT_DIRECTED_MULTI_COLUMN_NAME,
    SOURCE_COLUMN_ALIAS_NAMES,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_ALIAS_NAMES,
    TARGET_COLUMN_NAME,
    UNWEIGHTED_BIPARTITE_DEGREE_CENTRALITY_COLUMN_NAME,
    UNWEIGHTED_BIPARTITE_DEGREE_CENTRALITY_MULTI_COLUMN_NAME,
    UNWEIGHTED_DEGREE_CENTRALITY_COLUMN_NAME,
    UNWEIGHTED_DEGREE_CENTRALITY_MULTI_COLUMN_NAME,
)

if TYPE_CHECKING:
    import networkx as nx
    import polars as pl
    import pyarrow as pa
    from sqlalchemy import MetaData, Table  # noqa

    from kiara.models.values.value import Value
    from kiara_plugin.network_analysis.models import NetworkData
    from kiara_plugin.tabular.models import KiaraTable


def extract_networkx_nodes_as_table(
    graph: "nx.Graph",
    label_attr_name: Union[str, None, Iterable[str]] = None,
    ignore_attributes: Union[None, Iterable[str]] = None,
) -> Tuple["pa.Table", Dict[Hashable, int]]:
    """Extract the nodes of a networkx graph as a pyarrow table.

    Arguments:
        graph: the networkx graph
        label_attr_name: the name of the node attribute that should be used as label. If None, the node id is used.
        ignore_attributes: a list of node attributes that should be ignored and not added to the table

    Returns:
        a tuple with the table and a map containing the original node id as key and the newly created internal node id (int) as value
    """
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


def extract_networkx_edges_as_table(
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
            edge_columns.setdefault(k, []).append(v)  # type: ignore

    edges_table = pa.Table.from_pydict(mapping=edge_columns)

    return edges_table


def augment_nodes_table_with_connection_counts(
    nodes_table: Union["pa.Table", "pl.DataFrame"],
    edges_table: Union["pa.Table", "pl.DataFrame"],
) -> "pa.Table":
    import duckdb

    try:
        nodes_column_names = nodes_table.column_names  # type: ignore
    except Exception:
        nodes_column_names = nodes_table.columns  # type: ignore

    node_attr_columns = [f'"{x}"' for x in nodes_column_names if not x.startswith("_")]
    if node_attr_columns:
        other_columns = ", " + ", ".join(node_attr_columns)
    else:
        other_columns = ""

    query = f"""
    SELECT
         {NODE_ID_COLUMN_NAME},
         {LABEL_COLUMN_NAME},
         COALESCE(e1.{IN_DIRECTED_COLUMN_NAME}, 0) + COALESCE(e3.{OUT_DIRECTED_COLUMN_NAME}, 0) as {CONNECTIONS_COLUMN_NAME},
         COALESCE(e2.{IN_DIRECTED_MULTI_COLUMN_NAME}, 0) + COALESCE(e4.{OUT_DIRECTED_MULTI_COLUMN_NAME}, 0) as {CONNECTIONS_MULTI_COLUMN_NAME},
         COALESCE(e1.{IN_DIRECTED_COLUMN_NAME}, 0) as {IN_DIRECTED_COLUMN_NAME},
         COALESCE(e2.{IN_DIRECTED_MULTI_COLUMN_NAME}, 0) as {IN_DIRECTED_MULTI_COLUMN_NAME},
         COALESCE(e3.{OUT_DIRECTED_COLUMN_NAME}, 0) as {OUT_DIRECTED_COLUMN_NAME},
         COALESCE(e4.{OUT_DIRECTED_MULTI_COLUMN_NAME}, 0) as {OUT_DIRECTED_MULTI_COLUMN_NAME},
         CASE WHEN e5.{SOURCE_COLUMN_NAME} IS NOT NULL THEN true ELSE false END as {NODE_IS_SOURCE_COLUMN_NAME},
         CASE WHEN e6.{TARGET_COLUMN_NAME} IS NOT NULL THEN true ELSE false END as {NODE_IS_TARGET_COLUMN_NAME}
         {other_columns}
         FROM nodes_table n
         left join
           (SELECT {TARGET_COLUMN_NAME}, {COUNT_IDX_DIRECTED_COLUMN_NAME}, COUNT(*) as {IN_DIRECTED_COLUMN_NAME} from edges_table GROUP BY {TARGET_COLUMN_NAME}, {COUNT_IDX_DIRECTED_COLUMN_NAME}) e1
           on n.{NODE_ID_COLUMN_NAME} = e1.{TARGET_COLUMN_NAME} and e1.{COUNT_IDX_DIRECTED_COLUMN_NAME} = 1
         left join
           (SELECT {TARGET_COLUMN_NAME}, COUNT(*) as {IN_DIRECTED_MULTI_COLUMN_NAME} from edges_table GROUP BY {TARGET_COLUMN_NAME}) e2
           on n.{NODE_ID_COLUMN_NAME} = e2.{TARGET_COLUMN_NAME}
         left join
           (SELECT {SOURCE_COLUMN_NAME}, {COUNT_IDX_DIRECTED_COLUMN_NAME}, COUNT(*) as {OUT_DIRECTED_COLUMN_NAME} from edges_table GROUP BY {SOURCE_COLUMN_NAME}, {COUNT_IDX_DIRECTED_COLUMN_NAME}) e3
           on n.{NODE_ID_COLUMN_NAME} = e3.{SOURCE_COLUMN_NAME} and e3.{COUNT_IDX_DIRECTED_COLUMN_NAME} = 1
         left join
           (SELECT {SOURCE_COLUMN_NAME}, COUNT(*) as {OUT_DIRECTED_MULTI_COLUMN_NAME} from edges_table GROUP BY {SOURCE_COLUMN_NAME}) e4
           on n.{NODE_ID_COLUMN_NAME} = e4.{SOURCE_COLUMN_NAME}
        left join
           (SELECT DISTINCT {SOURCE_COLUMN_NAME} from edges_table) e5
           on n.{NODE_ID_COLUMN_NAME} = e5.{SOURCE_COLUMN_NAME}
        left join
           (SELECT DISTINCT {TARGET_COLUMN_NAME} from edges_table) e6
           on n.{NODE_ID_COLUMN_NAME} = e6.{TARGET_COLUMN_NAME}
        ORDER BY {NODE_ID_COLUMN_NAME}
    """
    nodes_table_result = duckdb.sql(query)  # noqa

    # we can avoid 'COUNT(*)' calls in the following  query
    nodes_table_rows = len(nodes_table)

    # get all ids of nodes that have '_is_source' set to True
    nodes_table_rows_sources_query = f"""
        SELECT DISTINCT {NODE_ID_COLUMN_NAME} FROM nodes_table_result
        WHERE {NODE_IS_SOURCE_COLUMN_NAME} = True
    """
    nodes_table_sources = duckdb.sql(nodes_table_rows_sources_query)

    # get all ids of nodes that have '_is_target' set to True
    nodes_table_rows_targets_query = f"""
        SELECT DISTINCT {NODE_ID_COLUMN_NAME} FROM nodes_table_result
        WHERE {NODE_IS_TARGET_COLUMN_NAME} = True
    """
    nodes_table_targets = duckdb.sql(nodes_table_rows_targets_query)

    nodes_table_rows_sources = len(nodes_table_sources)
    nodes_table_rows_targets = len(nodes_table_targets)

    centrality_query = f"""
    SELECT
         {NODE_ID_COLUMN_NAME},
         {LABEL_COLUMN_NAME},
         {CONNECTIONS_COLUMN_NAME},
         {CONNECTIONS_COLUMN_NAME} / {nodes_table_rows} AS {UNWEIGHTED_DEGREE_CENTRALITY_COLUMN_NAME},
         CASE
             WHEN {NODE_IS_SOURCE_COLUMN_NAME} = True THEN {OUT_DIRECTED_COLUMN_NAME} / {nodes_table_rows_sources}
             ELSE {IN_DIRECTED_COLUMN_NAME} / {nodes_table_rows_targets}
         END AS {UNWEIGHTED_BIPARTITE_DEGREE_CENTRALITY_COLUMN_NAME},
         {CONNECTIONS_MULTI_COLUMN_NAME},
         {CONNECTIONS_MULTI_COLUMN_NAME} / {nodes_table_rows} AS {UNWEIGHTED_DEGREE_CENTRALITY_MULTI_COLUMN_NAME},
         CASE
             WHEN {NODE_IS_SOURCE_COLUMN_NAME} = True THEN {OUT_DIRECTED_MULTI_COLUMN_NAME} / {nodes_table_rows_sources}
             ELSE {IN_DIRECTED_MULTI_COLUMN_NAME} / {nodes_table_rows_targets}
         END AS {UNWEIGHTED_BIPARTITE_DEGREE_CENTRALITY_MULTI_COLUMN_NAME},
         {IN_DIRECTED_COLUMN_NAME},
         {IN_DIRECTED_MULTI_COLUMN_NAME},
         {OUT_DIRECTED_COLUMN_NAME},
         {OUT_DIRECTED_MULTI_COLUMN_NAME},
         {NODE_IS_SOURCE_COLUMN_NAME},
         {NODE_IS_TARGET_COLUMN_NAME}
         {other_columns}
    FROM nodes_table_result
    """

    result = duckdb.sql(centrality_query)

    nodes_table_augmented = result.arrow()
    return nodes_table_augmented


def augment_edges_table_with_id_and_weights(
    edges_table: Union["pa.Table", "pl.DataFrame"],
) -> "pa.Table":
    """Augment the edges table with additional pre-computed columns for directed and undirected weights.."""

    import duckdb

    try:
        column_names = edges_table.column_names  # type: ignore
    except Exception:
        column_names = edges_table.columns  # type: ignore

    edge_attr_columns = [f'"{x}"' for x in column_names if not x.startswith("_")]
    if edge_attr_columns:
        other_columns = ", " + ", ".join(edge_attr_columns)
    else:
        other_columns = ""

    query = f"""
    SELECT
      ROW_NUMBER() OVER () -1 as {EDGE_ID_COLUMN_NAME},
      {SOURCE_COLUMN_NAME},
      {TARGET_COLUMN_NAME},
      COUNT(*) OVER (PARTITION BY {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME}) as {COUNT_DIRECTED_COLUMN_NAME},
      ROW_NUMBER(*) OVER (PARTITION BY {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME}) as {COUNT_IDX_DIRECTED_COLUMN_NAME},
      COUNT(*) OVER (PARTITION BY LEAST({SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME}), GREATEST({SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME})) as {COUNT_UNDIRECTED_COLUMN_NAME},
      ROW_NUMBER(*) OVER (PARTITION BY LEAST({SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME}), GREATEST({SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME})) as {COUNT_IDX_UNDIRECTED_COLUMN_NAME}
      {other_columns}
    FROM edges_table"""

    result = duckdb.sql(query)
    edges_table_augmented = result.arrow()

    return edges_table_augmented


def augment_tables_with_component_ids(
    nodes_table: "pa.Table", edges_table: "pa.Table"
) -> Tuple["pa.Table", "pa.Table"]:
    """Augment the nodes and edges table with a component id column.

    The component id is a unique id for each connected component in the graph. The id of the component is
    assigned in a deterministic way, based on the size of the components. The largest component gets id 0, the
    second largest 1, and so on.

    Should 2 components have the same amount of nodes, the component id is not deterministic anymore.
    """

    import polars as pl
    import pyarrow as pa
    import rustworkx as rx

    # create rustworkx graph
    graph = rx.PyGraph(multigraph=False)

    nodes_df: DataFrame = pl.from_arrow(nodes_table)  # type: ignore
    for row in nodes_df.select(NODE_ID_COLUMN_NAME).rows(named=True):
        node_id = row[NODE_ID_COLUMN_NAME]
        graph_node_id = graph.add_node(node_id)
        assert node_id == graph_node_id

    edges_df: DataFrame = pl.from_arrow(edges_table)  # type: ignore
    for row in edges_df.select(SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME).rows(named=True):
        if row[SOURCE_COLUMN_NAME] == row[TARGET_COLUMN_NAME]:
            continue

        source_id = row[SOURCE_COLUMN_NAME]
        target_id = row[TARGET_COLUMN_NAME]

        graph.add_edge(source_id, target_id, None)

    components = rx.connected_components(graph)
    nodes_idx = 0
    for col_name in nodes_table.column_names:
        if col_name.startswith("_"):
            nodes_idx += 1
            continue

    if len(components) == 1:
        components_column_nodes = pa.array([0] * len(nodes_table), type=pa.int64())
        nodes = nodes_table.add_column(
            nodes_idx, COMPONENT_ID_COLUMN_NAME, components_column_nodes
        )
        components_column_edges = pa.array([0] * len(edges_table), type=pa.int64())

        edges_idx = 0
        for col_name in edges_table.column_names:
            if col_name.startswith("_"):
                edges_idx += 1
                continue
        edges = edges_table.add_column(
            edges_idx, COMPONENT_ID_COLUMN_NAME, components_column_edges
        )
        return nodes, edges

    node_components = {}
    for idx, component in enumerate(sorted(components, key=len, reverse=True)):
        for node_id in component:
            node_components[node_id] = idx

    if len(node_components) != graph.num_nodes():
        raise KiaraException(
            "Number of nodes in component map does not match number of nodes in network data. This is most likely a bug."
        )

    components_column_nodes = pa.array(
        (node_components[node_id] for node_id in sorted(node_components.keys())),
        type=pa.int64(),
    )
    nodes = nodes_table.add_column(
        nodes_idx, COMPONENT_ID_COLUMN_NAME, components_column_nodes
    )

    try:
        column_names = edges_table.column_names  # type: ignore
    except Exception:
        column_names = edges_table.columns  # type: ignore

    computed_attr_columns = [f'"{x}"' for x in column_names if x.startswith("_")]
    computed_columns = ", ".join(computed_attr_columns)
    edge_attr_columns = [f'"{x}"' for x in column_names if not x.startswith("_")]
    if edge_attr_columns:
        other_columns = ", " + ", ".join(edge_attr_columns)
    else:
        other_columns = ""

    # a query that looks up the value of a SOURCE_COLUMN_NAME in edges_table in the
    # NODE_ID_COLUMN_NAME of nodes, and returns the component id from the nodes table
    query = f"""
    SELECT
        {computed_columns},
        n.{COMPONENT_ID_COLUMN_NAME} as {COMPONENT_ID_COLUMN_NAME}
        {other_columns}
    FROM edges_table e
    JOIN nodes n ON e.{SOURCE_COLUMN_NAME} = n.{NODE_ID_COLUMN_NAME}
    """

    edges = duckdb.sql(query)

    return nodes, edges.arrow()


def extract_network_data(network_data: Union["Value", "NetworkData"]) -> "NetworkData":
    from kiara.models.values.value import Value

    if isinstance(network_data, Value):
        assert network_data.data_type_name == "network_data"
        network_data = network_data.data
    return network_data  # type: ignore


def guess_column_name(
    table: Union["pa.Table", "KiaraTable", "Value"], suggestions: List[str]
) -> Union[str, None]:
    column_names: Union[List[str], None] = None

    if hasattr(table, "column_names"):
        column_names = table.column_names
    else:
        from kiara.models.values.value import Value

        if isinstance(table, Value):
            table_instance = table.data
            if hasattr(table_instance, "column_names"):
                column_names = table_instance.column_names

    if not column_names:
        return None

    for suggestion in suggestions:
        if suggestion in column_names:
            return suggestion

    for suggestion in suggestions:
        for column_name in column_names:
            if suggestion.lower() == column_name.lower():
                return column_name

    return None


def guess_node_id_column_name(
    nodes_table: Union["pa.Table", "KiaraTable", "Value"],
    suggestions: Union[None, List[str]] = None,
) -> Union[str, None]:
    if suggestions is None:
        suggestions = NODE_ID_ALIAS_NAMES
    return guess_column_name(table=nodes_table, suggestions=suggestions)


def guess_node_label_column_name(
    nodes_table: Union["pa.Table", "KiaraTable", "Value"],
    suggestions: Union[None, List[str]] = None,
) -> Union[str, None]:
    if suggestions is None:
        suggestions = LABEL_ALIAS_NAMES
    return guess_column_name(table=nodes_table, suggestions=suggestions)


def guess_source_column_name(
    edges_table: Union["pa.Table", "KiaraTable", "Value"],
    suggestions: Union[None, List[str]] = None,
) -> Union[str, None]:
    if suggestions is None:
        suggestions = SOURCE_COLUMN_ALIAS_NAMES
    return guess_column_name(table=edges_table, suggestions=suggestions)


def guess_target_column_name(
    edges_table: Union["pa.Table", "KiaraTable", "Value"],
    suggestions: Union[None, List[str]] = None,
) -> Union[str, None]:
    if suggestions is None:
        suggestions = TARGET_COLUMN_ALIAS_NAMES
    return guess_column_name(table=edges_table, suggestions=suggestions)
