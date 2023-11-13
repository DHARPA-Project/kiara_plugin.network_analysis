# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.network_analysis`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Iterable,
    List,
    Literal,
    Protocol,
    Type,
    TypeVar,
    Union,
)

from pydantic import BaseModel, Field

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
from kiara_plugin.network_analysis.defaults import (
    ATTRIBUTE_PROPERTY_KEY,
    CONNECTIONS_COLUMN_NAME,
    CONNECTIONS_MULTI_COLUMN_NAME,
    COUNT_DIRECTED_COLUMN_NAME,
    COUNT_IDX_DIRECTED_COLUMN_NAME,
    COUNT_IDX_UNDIRECTED_COLUMN_NAME,
    COUNT_UNDIRECTED_COLUMN_NAME,
    EDGE_ID_COLUMN_NAME,
    EDGES_TABLE_NAME,
    IN_DIRECTED_COLUMN_NAME,
    IN_DIRECTED_MULTI_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    NODES_TABLE_NAME,
    OUT_DIRECTED_COLUMN_NAME,
    OUT_DIRECTED_MULTI_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    GraphType,
)
from kiara_plugin.network_analysis.utils import (
    augment_edges_table_with_id_and_weights,
    augment_nodes_table_with_connection_counts,
    extract_networkx_edges_as_table,
    extract_networkx_nodes_as_table,
)
from kiara_plugin.tabular.models.tables import KiaraTables

if TYPE_CHECKING:
    import networkx as nx
    import pyarrow as pa
    import rustworkx as rx

    from kiara_plugin.network_analysis.models.metadata import (
        NetworkNodeAttributeMetadata,
    )
    from kiara_plugin.tabular.models.table import KiaraTable

NETWORKX_GRAPH_TYPE = TypeVar("NETWORKX_GRAPH_TYPE", bound="nx.Graph")
RUSTWORKX_GRAPH_TYPE = TypeVar("RUSTWORKX_GRAPH_TYPE", "rx.PyGraph", "rx.PyDiGraph")


class NodesCallback(Protocol):
    def __call__(self, _node_id: int, **kwargs) -> None:
        ...


class EdgesCallback(Protocol):
    def __call__(self, _source: int, _target: int, **kwargs) -> None:
        ...


class NetworkData(KiaraTables):
    """A helper class to access and query network datasets.

    This class provides different ways to access the underlying network data, most notably via sql and as networkx Graph object.

    Internally, network data is stored as 2 Arrow tables with the edges stored in a table called 'edges' and the nodes in a table called 'nodes'. The edges table must have (at least) the following columns: '_source', '_target'. The nodes table must have (at least) the following columns: '_id' (integer), '_label' (string).

    By convention, kiara will add columns prefixed with an underscore if the values in it have internal 'meaning', normal/original attributes are stored in columns without that prefix.
    """

    _kiara_model_id: ClassVar = "instance.network_data"

    @classmethod
    def create_network_data(
        cls,
        nodes_table: "pa.Table",
        edges_table: "pa.Table",
        augment_tables: bool = True,
        nodes_column_metadata: Union[Dict[str, Dict[str, KiaraModel]], None] = None,
        edges_column_metadata: Union[Dict[str, Dict[str, KiaraModel]], None] = None,
    ) -> "NetworkData":
        """Create a `NetworkData` instance from two Arrow tables.

        This method requires the nodes to have an "_id' column (int) as well as a '_label' one (utf8).
        The edges table needs at least a '_source' (int) and '_target' (int) column.

        This method will augment both tables with additional columns that are required for the internal representation (weights, degrees).
        """

        from kiara_plugin.network_analysis.models.metadata import (
            EDGE_COUNT_DUP_DIRECTED_COLUMN_METADATA,
            EDGE_COUNT_DUP_UNDIRECTED_COLUMN_METADATA,
            EDGE_ID_COLUMN_METADATA,
            EDGE_IDX_DUP_DIRECTED_COLUMN_METADATA,
            EDGE_IDX_DUP_UNDIRECTED_COLUMN_METADATA,
            EDGE_SOURCE_COLUMN_METADATA,
            EDGE_TARGET_COLUMN_METADATA,
            NODE_COUND_EDGES_MULTI_COLUMN_METADATA,
            NODE_COUNT_EDGES_COLUMN_METADATA,
            NODE_COUNT_IN_EDGES_COLUMN_METADATA,
            NODE_COUNT_IN_EDGES_MULTI_COLUMN_METADATA,
            NODE_COUNT_OUT_EDGES_COLUMN_METADATA,
            NODE_COUNT_OUT_EDGES_MULTI_COLUMN_METADATA,
            NODE_ID_COLUMN_METADATA,
            NODE_LABEL_COLUMN_METADATA,
        )

        if augment_tables:
            edges_table = augment_edges_table_with_id_and_weights(edges_table)
            nodes_table = augment_nodes_table_with_connection_counts(
                nodes_table, edges_table
            )

        network_data: NetworkData = cls.create_tables(
            {NODES_TABLE_NAME: nodes_table, EDGES_TABLE_NAME: edges_table}
        )

        # set default column metadata
        network_data.edges.set_column_metadata(
            EDGE_ID_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_ID_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            SOURCE_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_SOURCE_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            TARGET_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_TARGET_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            COUNT_DIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_COUNT_DUP_DIRECTED_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            COUNT_IDX_DIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_IDX_DUP_DIRECTED_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            COUNT_UNDIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_COUNT_DUP_UNDIRECTED_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            COUNT_IDX_UNDIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_IDX_DUP_UNDIRECTED_COLUMN_METADATA,
            overwrite_existing=False,
        )

        network_data.nodes.set_column_metadata(
            NODE_ID_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_ID_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            LABEL_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_LABEL_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            CONNECTIONS_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_EDGES_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            CONNECTIONS_MULTI_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUND_EDGES_MULTI_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            IN_DIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_IN_EDGES_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            IN_DIRECTED_MULTI_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_IN_EDGES_MULTI_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            OUT_DIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_OUT_EDGES_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            OUT_DIRECTED_MULTI_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_OUT_EDGES_MULTI_COLUMN_METADATA,
            overwrite_existing=False,
        )

        if nodes_column_metadata is not None:
            for col_name, col_meta in nodes_column_metadata.items():
                for prop_name, prop_value in col_meta.items():
                    network_data.nodes.set_column_metadata(
                        col_name, prop_name, prop_value, overwrite_existing=True
                    )
        if edges_column_metadata is not None:
            for col_name, col_meta in edges_column_metadata.items():
                for prop_name, prop_value in col_meta.items():
                    network_data.edges.set_column_metadata(
                        col_name, prop_name, prop_value, overwrite_existing=True
                    )

        return network_data

    @classmethod
    def from_filtered_nodes(
        cls, network_data: "NetworkData", nodes_list: List[int]
    ) -> "NetworkData":

        import duckdb
        import polars as pl

        node_columns = [NODE_ID_COLUMN_NAME, LABEL_COLUMN_NAME]
        for column_name, metadata in network_data.nodes.column_metadata.items():
            attr_prop: Union[None, NetworkNodeAttributeMetadata] = metadata.get(  # type: ignore
                ATTRIBUTE_PROPERTY_KEY, None
            )
            if attr_prop is None or not attr_prop.computed_attribute:
                node_columns.append(column_name)

        node_list_str = ", ".join([str(n) for n in nodes_list])

        nodes_table = network_data.nodes.arrow_table  # noqa
        nodes_query = f"SELECT {', '.join(node_columns)} FROM nodes_table n WHERE n.{NODE_ID_COLUMN_NAME} IN ({node_list_str})"

        nodes_result = duckdb.sql(nodes_query).pl()

        edges_table = network_data.edges.arrow_table  # noqa
        edge_columns = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
        for column_name, metadata in network_data.edges.column_metadata.items():
            attr_prop = metadata.get(ATTRIBUTE_PROPERTY_KEY, None)  # type: ignore
            if attr_prop is None or not attr_prop.computed_attribute:
                edge_columns.append(column_name)

        edges_query = f"SELECT {', '.join(edge_columns)} FROM edges_table WHERE {SOURCE_COLUMN_NAME} IN ({node_list_str}) OR {TARGET_COLUMN_NAME} IN ({node_list_str})"

        edges_result = duckdb.sql(edges_query).pl()

        nodes_idx_colum = range(len(nodes_result))
        old_idx_column = nodes_result[NODE_ID_COLUMN_NAME]

        repl_map = dict(zip(old_idx_column.to_list(), nodes_idx_colum))
        nodes_result = nodes_result.with_columns(
            pl.col(NODE_ID_COLUMN_NAME).map_dict(repl_map)
        )

        edges_result = edges_result.with_columns(
            pl.col(SOURCE_COLUMN_NAME).map_dict(repl_map),
            pl.col(TARGET_COLUMN_NAME).map_dict(repl_map),
        )

        filtered = NetworkData.create_network_data(
            nodes_table=nodes_result, edges_table=edges_result
        )
        return filtered

    @classmethod
    def create_from_networkx_graph(
        cls,
        graph: "nx.Graph",
        label_attr_name: Union[str, None] = None,
        ignore_node_attributes: Union[Iterable[str], None] = None,
    ) -> "NetworkData":
        """Create a `NetworkData` instance from a networkx Graph object."""

        # TODO: should we also index nodes/edges attributes?

        nodes_table, node_id_map = extract_networkx_nodes_as_table(
            graph=graph,
            label_attr_name=label_attr_name,
            ignore_attributes=ignore_node_attributes,
        )

        edges_table = extract_networkx_edges_as_table(graph, node_id_map)

        network_data = NetworkData.create_network_data(
            nodes_table=nodes_table, edges_table=edges_table
        )

        return network_data

    @property
    def edges(self) -> "KiaraTable":
        """Return the edges table."""

        return self.tables[EDGES_TABLE_NAME]

    @property
    def nodes(self) -> "KiaraTable":
        """Return the nodes table."""

        return self.tables[NODES_TABLE_NAME]

    @property
    def num_nodes(self):
        """Return the number of nodes in the network data."""

        return self.nodes.num_rows

    @property
    def num_edges(self):
        """Return the number of edges in the network data."""

        return self.edges.num_rows

    def query_edges(
        self, sql_query: str, relation_name: str = EDGES_TABLE_NAME
    ) -> "pa.Table":
        """Query the edges table using SQL.

        The table name to use in the query defaults to 'edges', but can be changed using the 'relation_name' argument.
        """

        import duckdb

        con = duckdb.connect()
        edges = self.edges.arrow_table  # noqa: F841
        if relation_name != EDGES_TABLE_NAME:
            sql_query = sql_query.replace(relation_name, EDGES_TABLE_NAME)

        result = con.execute(sql_query)
        return result.arrow()

    def query_nodes(
        self, sql_query: str, relation_name: str = NODES_TABLE_NAME
    ) -> "pa.Table":
        """Query the nodes table using SQL.

        The table name to use in the query defaults to 'nodes', but can be changed using the 'relation_name' argument.
        """

        import duckdb

        con = duckdb.connect()
        nodes = self.nodes.arrow_table  # noqa
        if relation_name != NODES_TABLE_NAME:
            sql_query = sql_query.replace(relation_name, NODES_TABLE_NAME)

        result = con.execute(sql_query)
        return result.arrow()

    def _calculate_node_attributes(
        self, incl_node_attributes: Union[bool, str, Iterable[str]]
    ) -> List[str]:
        """Calculate the node attributes that should be included in the output."""

        if incl_node_attributes is False:
            node_attr_names: List[str] = [NODE_ID_COLUMN_NAME, LABEL_COLUMN_NAME]
        else:
            all_node_attr_names: List[str] = self.nodes.column_names  # type: ignore
            if incl_node_attributes is True:
                node_attr_names = [NODE_ID_COLUMN_NAME]
                node_attr_names.extend((x for x in all_node_attr_names if x != NODE_ID_COLUMN_NAME))  # type: ignore
            elif isinstance(incl_node_attributes, str):
                if incl_node_attributes not in all_node_attr_names:
                    raise KiaraException(
                        f"Can't include node attribute {incl_node_attributes}: not part of the available attributes ({', '.join(all_node_attr_names)})."
                    )
                node_attr_names = [NODE_ID_COLUMN_NAME, incl_node_attributes]
            else:
                node_attr_names = [NODE_ID_COLUMN_NAME]
                for attr_name in incl_node_attributes:
                    if incl_node_attributes not in all_node_attr_names:
                        raise KiaraException(
                            f"Can't include node attribute {incl_node_attributes}: not part of the available attributes ({', '.join(all_node_attr_names)})."
                        )
                    node_attr_names.append(attr_name)  # type: ignore

        return node_attr_names

    def _calculate_edge_attributes(
        self, incl_edge_attributes: Union[bool, str, Iterable[str]]
    ) -> List[str]:
        """Calculate the edge attributes that should be included in the output."""

        if incl_edge_attributes is False:
            edge_attr_names: List[str] = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
        else:
            all_edge_attr_names: List[str] = self.edges.column_names  # type: ignore
            if incl_edge_attributes is True:
                edge_attr_names = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
                edge_attr_names.extend(
                    (
                        x
                        for x in all_edge_attr_names
                        if x not in (SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME)
                    )
                )  # type: ignore
            elif isinstance(incl_edge_attributes, str):
                if incl_edge_attributes not in all_edge_attr_names:
                    raise KiaraException(
                        f"Can't include edge attribute {incl_edge_attributes}: not part of the available attributes ({', '.join(all_edge_attr_names)})."
                    )
                edge_attr_names = [
                    SOURCE_COLUMN_NAME,
                    TARGET_COLUMN_NAME,
                    incl_edge_attributes,
                ]
            else:
                edge_attr_names = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
                for attr_name in incl_edge_attributes:
                    if incl_edge_attributes not in all_edge_attr_names:
                        raise KiaraException(
                            f"Can't include edge attribute {incl_edge_attributes}: not part of the available attributes ({', '.join(all_edge_attr_names)})."
                        )
                    edge_attr_names.append(attr_name)  # type: ignore

        return edge_attr_names

    def retrieve_graph_data(
        self,
        nodes_callback: Union[NodesCallback, None] = None,
        edges_callback: Union[EdgesCallback, None] = None,
        incl_node_attributes: Union[bool, str, Iterable[str]] = False,
        incl_edge_attributes: Union[bool, str, Iterable[str]] = False,
        omit_self_loops: bool = False,
    ):
        """Retrieve graph data from the sqlite database, and call the specified callbacks for each node and edge.

        First the nodes will be processed, then the edges, if that does not suit your needs you can just use this method twice, and set the callback you don't need to None.

        The nodes_callback will be called with the following arguments:
            - node_id: the id of the node (int)
            - if False: nothing else
            - if True: all node attributes, in the order they are defined in the table schema
            - if str: the value of the specified node attribute
            - if Iterable[str]: the values of the specified node attributes, in the order they are specified

        The edges_callback will be called with the following aruments:
            - source_id: the id of the source node (int)
            - target_id: the id of the target node (int)
            - if False: nothing else
            - if True: all edge attributes, in the order they are defined in the table schema
            - if str: the value of the specified edge attribute
            - if Iterable[str]: the values of the specified edge attributes, in the order they are specified

        """

        if nodes_callback is not None:
            node_attr_names = self._calculate_node_attributes(incl_node_attributes)

            nodes_df = self.nodes.to_polars_dataframe()
            for row in nodes_df.select(*node_attr_names).rows(named=True):
                nodes_callback(**row)  # type: ignore

        if edges_callback is not None:
            edge_attr_names = self._calculate_edge_attributes(incl_edge_attributes)

            edges_df = self.edges.to_polars_dataframe()
            for row in edges_df.select(*edge_attr_names).rows(named=True):
                if (
                    omit_self_loops
                    and row[SOURCE_COLUMN_NAME] == row[TARGET_COLUMN_NAME]
                ):
                    continue
                edges_callback(**row)  # type: ignore

    def as_networkx_graph(
        self,
        graph_type: Type[NETWORKX_GRAPH_TYPE],
        incl_node_attributes: Union[bool, str, Iterable[str]] = False,
        incl_edge_attributes: Union[bool, str, Iterable[str]] = False,
        omit_self_loops: bool = False,
    ) -> NETWORKX_GRAPH_TYPE:
        """Return the network data as a networkx graph object.

        Arguments:
            graph_type: the networkx Graph class to use
            incl_node_attributes: if True, all node attributes are included in the graph, if False, none are, otherwise the specified attributes are included
            incl_edge_attributes: if True, all edge attributes are included in the graph, if False, none are, otherwise the specified attributes are included
            omit_self_loops: if False, self-loops are included in the graph, otherwise they are not added to the resulting graph (nodes that are only connected to themselves are still included)

        """

        graph: NETWORKX_GRAPH_TYPE = graph_type()

        def add_node(_node_id: int, **attrs):
            graph.add_node(_node_id, **attrs)

        def add_edge(_source: int, _target: int, **attrs):
            graph.add_edge(_source, _target, **attrs)

        self.retrieve_graph_data(
            nodes_callback=add_node,
            edges_callback=add_edge,
            incl_node_attributes=incl_node_attributes,
            incl_edge_attributes=incl_edge_attributes,
            omit_self_loops=omit_self_loops,
        )

        return graph

    def as_rustworkx_graph(
        self,
        graph_type: Type[RUSTWORKX_GRAPH_TYPE],
        multigraph: bool = False,
        incl_node_attributes: Union[bool, str, Iterable[str]] = False,
        incl_edge_attributes: Union[bool, str, Iterable[str]] = False,
        omit_self_loops: bool = False,
        attach_node_id_map: bool = False,
    ) -> RUSTWORKX_GRAPH_TYPE:
        """
        Return the network data as a rustworkx graph object.

        Be aware that the node ids in the rustworks graph might not match up with the values of the _node_id column of
        the original network_data. The original _node_id will be set as an attribute (`_node_id`) on the nodes.

        Arguments:
            graph_type: the rustworkx Graph class to use
            multigraph: if True, a Multi(Di)Graph is returned, otherwise a normal (Di)Graph
            incl_node_attributes: if True, all node attributes are included in the graph, if False, none are, otherwise the specified attributes are included
            incl_edge_attributes: if True, all edge attributes are included in the graph, if False, none are, otherwise the specified attributes are included
            omit_self_loops: if False, self-loops are included in the graph, otherwise they are not added to the resulting graph (nodes that are only connected to themselves are still included)
            attach_node_id_map: if True, add the dict describing how the graph node ids (key) are mapped to the original node id of the network data, under the 'node_id_map' key in the graph's attributes
        """

        from bidict import bidict

        graph = graph_type(multigraph=multigraph)

        # rustworkx uses 0-based integer indexes, so we don't neeed to look up the node ids (unless we want to
        # include node attributes)

        self._calculate_node_attributes(incl_node_attributes)[1:]
        self._calculate_edge_attributes(incl_edge_attributes)[2:]

        # we can use a 'global' dict here because we know the nodes are processed before the edges
        node_map: bidict = bidict()

        def add_node(_node_id: int, **attrs):
            data = {NODE_ID_COLUMN_NAME: _node_id}
            data.update(attrs)

            graph_node_id = graph.add_node(data)

            node_map[graph_node_id] = _node_id
            # if not _node_id == graph_node_id:
            #     raise Exception("Internal error: node ids don't match")

        def add_edge(_source: int, _target: int, **attrs):

            source = node_map[_source]
            target = node_map[_target]
            if not attrs:
                graph.add_edge(source, target, None)
            else:
                graph.add_edge(source, target, attrs)

        self.retrieve_graph_data(
            nodes_callback=add_node,
            edges_callback=add_edge,
            incl_node_attributes=incl_node_attributes,
            incl_edge_attributes=incl_edge_attributes,
            omit_self_loops=omit_self_loops,
        )

        if attach_node_id_map:
            graph.attrs = {"node_id_map": node_map}  # type: ignore

        return graph


class GraphProperties(BaseModel):
    """Properties of graph data, if interpreted as a specific graph type."""

    number_of_edges: int = Field(description="The number of edges.")
    parallel_edges: int = Field(
        description="The number of parallel edges (if 'multi' graph type).", default=0
    )


class NetworkGraphProperties(ValueMetadata):
    """Network data stats."""

    _metadata_key: ClassVar[str] = "network_data"

    number_of_nodes: int = Field(description="Number of nodes in the network graph.")
    properties_by_graph_type: Dict[  # type: ignore
        Literal[
            GraphType.DIRECTED.value,
            GraphType.UNDIRECTED.value,
            GraphType.UNDIRECTED_MULTI.value,
            GraphType.DIRECTED_MULTI.value,
        ],
        GraphProperties,
    ] = Field(description="Properties of the network data, by graph type.")
    number_of_self_loops: int = Field(
        description="Number of edges where source and target point to the same node."
    )

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["network_data"]

    @classmethod
    def create_value_metadata(cls, value: Value) -> "NetworkGraphProperties":

        network_data: NetworkData = value.data

        num_rows = network_data.num_nodes
        num_edges = network_data.num_edges

        # query_num_edges_directed = f"SELECT COUNT(*) FROM (SELECT DISTINCT {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME} FROM {EDGES_TABLE_NAME})"
        query_num_edges_directed = f"SELECT COUNT(*) FROM {EDGES_TABLE_NAME} WHERE {COUNT_IDX_DIRECTED_COLUMN_NAME} = 1"

        num_edges_directed_result = network_data.query_edges(query_num_edges_directed)
        num_edges_directed = num_edges_directed_result.columns[0][0].as_py()

        query_num_edges_undirected = f"SELECT COUNT(*) FROM {EDGES_TABLE_NAME} WHERE {COUNT_IDX_UNDIRECTED_COLUMN_NAME} = 1"
        num_edges_undirected_result = network_data.query_edges(
            query_num_edges_undirected
        )
        num_edges_undirected = num_edges_undirected_result.columns[0][0].as_py()

        self_loop_query = f"SELECT count(*) FROM {EDGES_TABLE_NAME} WHERE {SOURCE_COLUMN_NAME} = {TARGET_COLUMN_NAME}"
        self_loop_result = network_data.query_edges(self_loop_query)
        num_self_loops = self_loop_result.columns[0][0].as_py()

        num_parallel_edges_directed_query = f"SELECT COUNT(*) FROM {EDGES_TABLE_NAME} WHERE {COUNT_IDX_DIRECTED_COLUMN_NAME} = 2"
        num_parallel_edges_directed_result = network_data.query_edges(
            num_parallel_edges_directed_query
        )
        num_parallel_edges_directed = num_parallel_edges_directed_result.columns[0][
            0
        ].as_py()

        num_parallel_edges_undirected_query = f"SELECT COUNT(*) FROM {EDGES_TABLE_NAME} WHERE {COUNT_IDX_UNDIRECTED_COLUMN_NAME} = 2"
        num_parallel_edges_undirected_result = network_data.query_edges(
            num_parallel_edges_undirected_query
        )
        num_parallel_edges_undirected = num_parallel_edges_undirected_result.columns[0][
            0
        ].as_py()

        directed_props = GraphProperties(number_of_edges=num_edges_directed)
        undirected_props = GraphProperties(number_of_edges=num_edges_undirected)
        directed_multi_props = GraphProperties(
            number_of_edges=num_edges, parallel_edges=num_parallel_edges_directed
        )
        undirected_multi_props = GraphProperties(
            number_of_edges=num_edges, parallel_edges=num_parallel_edges_undirected
        )

        props = {
            GraphType.DIRECTED.value: directed_props,
            GraphType.DIRECTED_MULTI.value: directed_multi_props,
            GraphType.UNDIRECTED.value: undirected_props,
            GraphType.UNDIRECTED_MULTI.value: undirected_multi_props,
        }

        result = cls(
            number_of_nodes=num_rows,
            properties_by_graph_type=props,
            number_of_self_loops=num_self_loops,
        )
        return result
