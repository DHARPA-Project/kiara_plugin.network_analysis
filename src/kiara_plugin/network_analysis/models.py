# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.network_analysis`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
import atexit
import os
import shutil
import tempfile
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pydantic import BaseModel, Field, PrivateAttr
from sqlalchemy import Table

from kiara.exceptions import KiaraException
from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
from kiara_plugin.network_analysis.defaults import (
    DEFAULT_NETWORK_DATA_CHUNK_SIZE,
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    RANKING_COLUNN_NAME,
    RANKING_TABLE_NAME,
    RANKING_VALUE_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    NetworkDataTableType,
)
from kiara_plugin.network_analysis.utils import (
    extract_edges_as_table,
    extract_nodes_as_table,
    insert_table_data_into_network_graph,
)
from kiara_plugin.tabular.defaults import SqliteDataType
from kiara_plugin.tabular.models.db import KiaraDatabase, SqliteTableSchema
from kiara_plugin.tabular.utils import create_sqlite_schema_data_from_arrow_table

if TYPE_CHECKING:
    import networkx as nx
    import rustworkx as rx


class GraphTypesEnum(Enum):
    undirected = "undirected"
    directed = "directed"
    multi_directed = "multi_directed"
    multi_undirected = "multi_undirected"


RUSTWORKX_GRAPH_TYPE = TypeVar("RUSTWORKX_GRAPH_TYPE", "rx.PyGraph", "rx.PyDiGraph")


class GraphRankingData(KiaraDatabase):
    """A model containing ranking data (and metadata) for a 'NetworkData' instsance."""

    _kiara_model_id = "instance.network_data.ranking"

    @classmethod
    def create_graph_ranking_data_from_sqlite(
        cls, db_file_path: str
    ) -> "GraphRankingData":
        """Create a `NetworkData` instance from a sqlite database file."""

        return cls.create_graph_ranking_data_from_database(
            KiaraDatabase(db_file_path=db_file_path)
        )

    @classmethod
    def create_graph_ranking_data_from_database(
        cls, db: KiaraDatabase
    ) -> "GraphRankingData":
        """Create a `NetworkData` instance from a `KiaraDatabase` instance."""

        # insp = db.get_sqlalchemy_inspector()
        # r_cols = insp.get_columns(RANKING_TABLE_NAME)
        #
        # ranking_columns: Dict[str, SqliteDataType] = {}
        # r_nullables = []
        # for c in ranking_columns:
        #     d_type = str(c["type"])
        #     edges_columns[c["name"]] = d_type  # type: ignore
        #     if c["nullable"]:
        #         r_nullables.append(c["name"])
        #
        # ranking_schema = SqliteTableSchema(
        #     columns=ranking_columns, nullable_columns=r_nullables
        # )
        nd = GraphRankingData(
            db_file_path=db.db_file_path,
        )
        return nd

    @classmethod
    def create_from_ranking_dict(
        cls, ranking_dict: Dict[int, int]
    ) -> "GraphRankingData":

        ranking_columns = {
            ID_COLUMN_NAME: "INTEGER",
            RANKING_COLUNN_NAME: "INTEGER",
            RANKING_VALUE_COLUMN_NAME: "REAL",
        }

        ranking_table_schema = SqliteTableSchema(
            columns=ranking_columns,
            index_columns=[ID_COLUMN_NAME, RANKING_COLUNN_NAME],
            nullable_columns=[],
            unique_columns=[ID_COLUMN_NAME],
            primary_key=ID_COLUMN_NAME,
        )

        db = GraphRankingData.create_in_temp_dir()
        db._unlock_db()
        engine = db.get_sqlalchemy_engine()

        _table = ranking_table_schema.create_table(
            table_name=RANKING_TABLE_NAME, engine=engine
        )

        rank_data = []
        count = 0
        previous = None
        rank = 0
        for k, v in sorted(ranking_dict.items(), key=lambda x: x[1], reverse=True):
            count += 1
            if v != previous:
                rank += count
                previous = v
                count = 0
            rank_data.append(
                {
                    ID_COLUMN_NAME: k,
                    RANKING_COLUNN_NAME: rank,
                    RANKING_VALUE_COLUMN_NAME: v,
                }
            )

        with engine.connect() as conn:
            conn.execute(_table.insert(), rank_data)
            conn.commit()

        db._lock_db()
        return db


class NetworkData(KiaraDatabase):
    """A helper class to access and query network datasets.

    This class provides different ways to access the underlying network data, most notably via sql and as networkx Graph object.

    Internally, network data is stored in a sqlite database with the edges stored in a table called 'edges' and the nodes in a table called 'nodes'. The edges table must have (at least) the following columns: '_source', '_target'. The nodes table must have (at least) the following columns: '_id' (integer), '_label' (string).

    """

    _kiara_model_id = "instance.network_data"

    @classmethod
    def create_from_networkx_graph(
        cls,
        graph: "nx.Graph",
        label_attr_name: Union[str, None] = None,
        ignore_node_attributes: Union[Iterable[str], None] = None,
    ) -> "NetworkData":
        """Create a `NetworkData` instance from a networkx Graph object."""

        # TODO: should we also index nodes/edges attributes?

        nodes_table, node_id_map = extract_nodes_as_table(
            graph=graph,
            label_attr_name=label_attr_name,
            ignore_attributes=ignore_node_attributes,
        )

        index_columns = [ID_COLUMN_NAME, LABEL_COLUMN_NAME]
        unique_columns = [ID_COLUMN_NAME]
        nodes_schema = create_sqlite_schema_data_from_arrow_table(
            nodes_table,
            index_columns=index_columns,
            unique_columns=unique_columns,
            primary_key=ID_COLUMN_NAME,
        )

        edges_table = extract_edges_as_table(graph, node_id_map)
        index_columns = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
        edges_schema = create_sqlite_schema_data_from_arrow_table(
            edges_table, index_columns=index_columns
        )

        network_data = NetworkData.create_network_data_in_temp_dir(
            schema_edges=edges_schema, schema_nodes=nodes_schema, keep_unlocked=True
        )
        insert_table_data_into_network_graph(
            network_data=network_data,
            edges_table=edges_table,
            nodes_table=nodes_table,
            chunk_size=DEFAULT_NETWORK_DATA_CHUNK_SIZE,
        )
        network_data._lock_db()

        return network_data

    @classmethod
    def create_network_data_from_sqlite(cls, db_file_path: str) -> "NetworkData":
        """Create a `NetworkData` instance from a sqlite database file."""

        return cls.create_network_data_from_database(
            KiaraDatabase(db_file_path=db_file_path)
        )

    @classmethod
    def create_network_data_from_database(cls, db: KiaraDatabase) -> "NetworkData":
        """Create a `NetworkData` instance from a `KiaraDatabase` instance."""

        insp = db.get_sqlalchemy_inspector()
        e_cols = insp.get_columns(NetworkDataTableType.EDGES.value)
        edges_columns: Dict[str, SqliteDataType] = {}
        edg_nullables = []
        for c in e_cols:
            d_type = str(c["type"])
            edges_columns[c["name"]] = d_type  # type: ignore
            if c["nullable"]:
                edg_nullables.append(c["name"])

        edge_schema = SqliteTableSchema(
            columns=edges_columns, nullable_columns=edg_nullables
        )

        n_cols = insp.get_columns(NetworkDataTableType.NODES.value)
        node_columns: Dict[str, SqliteDataType] = {}
        nd_nullables = []
        for c in n_cols:
            d_type = str(c["type"])
            node_columns[c["name"]] = d_type  # type: ignore
            if c["nullable"]:
                nd_nullables.append(c["name"])

        node_schema = SqliteTableSchema(
            columns=node_columns, nullable_columns=nd_nullables
        )

        # TODO: parse indexes/primary keys

        nd = NetworkData(
            db_file_path=db.db_file_path,
            edges_schema=edge_schema,
            nodes_schema=node_schema,
        )
        return nd

    @classmethod
    def create_network_data_in_temp_dir(
        cls,
        schema_edges: Union[None, SqliteTableSchema, Mapping] = None,
        schema_nodes: Union[None, SqliteTableSchema, Mapping] = None,
        keep_unlocked: bool = False,
    ):
        """Create a new, empty `NetworkData` instance in a temporary directory."""

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "network_data.sqlite")

        def cleanup():
            try:
                os.unlink(db_path)
            except Exception:
                pass

        atexit.register(cleanup)

        if schema_edges is None:
            suggested_id_type = "TEXT"
            if schema_nodes is not None:
                if isinstance(schema_nodes, Mapping):
                    suggested_id_type = schema_nodes.get(ID_COLUMN_NAME, "TEXT")
                elif isinstance(schema_nodes, SqliteTableSchema):
                    suggested_id_type = schema_nodes.columns.get(ID_COLUMN_NAME, "TEXT")

            edges_schema = SqliteTableSchema.construct(
                columns={
                    SOURCE_COLUMN_NAME: suggested_id_type,  # type: ignore
                    TARGET_COLUMN_NAME: suggested_id_type,  # type: ignore
                }
            )
        elif isinstance(schema_edges, Mapping):
            edges_schema = SqliteTableSchema(**schema_edges)
        elif not isinstance(schema_edges, SqliteTableSchema):
            raise ValueError(
                f"Invalid data type for edges schema: {type(schema_edges)}"
            )
        else:
            edges_schema = schema_edges

        if (
            edges_schema.columns[SOURCE_COLUMN_NAME]
            != edges_schema.columns[TARGET_COLUMN_NAME]
        ):
            raise ValueError(
                f"Invalid edges schema, source and edges columns have different type: {edges_schema.columns[SOURCE_COLUMN_NAME]} != {edges_schema.columns[TARGET_COLUMN_NAME]}"
            )

        if schema_nodes is None:
            schema_nodes = SqliteTableSchema.construct(
                columns={
                    ID_COLUMN_NAME: edges_schema.columns[SOURCE_COLUMN_NAME],
                    LABEL_COLUMN_NAME: "TEXT",
                }
            )

        if isinstance(schema_nodes, Mapping):
            nodes_schema = SqliteTableSchema(**schema_nodes)
        elif isinstance(schema_nodes, SqliteTableSchema):
            nodes_schema = schema_nodes
        else:
            raise ValueError(
                f"Invalid data type for nodes schema: {type(schema_edges)}"
            )

        if ID_COLUMN_NAME not in nodes_schema.columns.keys():
            raise ValueError(
                f"Invalid nodes schema: missing '{ID_COLUMN_NAME}' column."
            )

        if LABEL_COLUMN_NAME not in nodes_schema.columns.keys():
            nodes_schema.columns[LABEL_COLUMN_NAME] = "TEXT"
        elif nodes_schema.columns[LABEL_COLUMN_NAME] != "TEXT":
            raise ValueError(
                f"Invalid nodes schema, '{LABEL_COLUMN_NAME}' column must be of type 'TEXT', not '{nodes_schema.columns[LABEL_COLUMN_NAME]}'."
            )

        if (
            nodes_schema.columns[ID_COLUMN_NAME]
            != edges_schema.columns[SOURCE_COLUMN_NAME]
        ):
            raise ValueError(
                f"Invalid nodes schema, id column has different type to edges source/target columns: {nodes_schema.columns[ID_COLUMN_NAME]} != {edges_schema.columns[SOURCE_COLUMN_NAME]}"
            )

        db = cls(
            db_file_path=db_path, edges_schema=edges_schema, nodes_schema=nodes_schema
        )
        db.create_if_not_exists()

        db._unlock_db()
        engine = db.get_sqlalchemy_engine()
        db.edges_schema.create_table(
            table_name=NetworkDataTableType.EDGES.value, engine=engine
        )
        db.nodes_schema.create_table(
            table_name=NetworkDataTableType.NODES.value, engine=engine
        )
        if not keep_unlocked:
            db._lock_db()

        return db

    edges_schema: SqliteTableSchema = Field(
        description="The schema information for the edges table."
    )
    nodes_schema: SqliteTableSchema = Field(
        description="The schema information for the nodes table."
    )

    _nodes_table_obj: Union[Table, None] = PrivateAttr(default=None)
    _edges_table_obj: Union[Table, None] = PrivateAttr(default=None)

    def _invalidate_other(self):
        self._nodes_table_obj = None
        self._edges_table_obj = None

    def get_sqlalchemy_nodes_table(self) -> Table:
        """Return the sqlalchemy nodes table instance for this network datab."""

        if self._nodes_table_obj is not None:
            return self._nodes_table_obj

        self._nodes_table_obj = Table(
            NetworkDataTableType.NODES.value,
            self.get_sqlalchemy_metadata(),
            autoload_with=self.get_sqlalchemy_engine(),
        )
        return self._nodes_table_obj

    def get_sqlalchemy_edges_table(self) -> Table:
        """Return the sqlalchemy edges table instance for this network datab."""

        if self._edges_table_obj is not None:
            return self._edges_table_obj

        self._edges_table_obj = Table(
            NetworkDataTableType.EDGES.value,
            self.get_sqlalchemy_metadata(),
            autoload_with=self.get_sqlalchemy_engine(),
        )
        return self._edges_table_obj

    def clone(self) -> "NetworkData":
        """Clone the current network data instance."""

        temp_f = tempfile.mkdtemp()

        new_file = shutil.copy2(self.db_file_path, temp_f)

        def cleanup():
            try:
                os.unlink(new_file)
            except Exception:
                pass

        atexit.register(cleanup)

        result = self.__class__.create_network_data_from_sqlite(new_file)
        return result

    def insert_nodes(self, *nodes: Mapping[str, Any]):
        """Add nodes to a network data item.

        Arguments:
            nodes: a list of dicts with the nodes
        """

        engine = self.get_sqlalchemy_engine()
        nodes_table = self.get_sqlalchemy_nodes_table()

        with engine.connect() as conn:
            with conn.begin():
                conn.execute(nodes_table.insert(), nodes)

    def insert_edges(
        self,
        *edges: Mapping[str, Any],
        existing_node_ids: Union[Iterable[int], None] = None,
    ) -> Set[int]:
        """Add edges to a network data item.

        All the edges need to have their node-ids registered already.

        Arguments:
            edges: a list of dicts with the edges
            existing_node_ids: a set of ids that can be assumed to already exist, this is mainly for performance reasons

        Returns:
            a unique set of all node ids contained in source and target columns
        """

        if existing_node_ids is None:
            # TODO: run query
            existing_node_ids = set()
        else:
            existing_node_ids = set(existing_node_ids)

        required_node_ids = {edge[SOURCE_COLUMN_NAME] for edge in edges}
        required_node_ids.update(edge[TARGET_COLUMN_NAME] for edge in edges)

        node_ids = list(required_node_ids.difference(existing_node_ids))

        if node_ids:
            self.insert_nodes(
                *(
                    {ID_COLUMN_NAME: node_id, LABEL_COLUMN_NAME: str(node_id)}
                    for node_id in node_ids
                )
            )

        engine = self.get_sqlalchemy_engine()
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(self.get_sqlalchemy_edges_table().insert(), edges)

        return required_node_ids

    def get_number_of_nodes(self):
        """Return the number of nodes in the network data."""

        from sqlalchemy import text

        with self.get_sqlalchemy_engine().connect() as con:
            result = con.execute(
                text(f"SELECT count(*) from {NetworkDataTableType.NODES.value}")
            )
            num_rows = result.fetchone()[0]

        return num_rows

    def as_networkx_graph(
        self,
        graph_type: Type["nx.Graph"],
        incl_node_attributes: Union[bool, str, Iterable[str]] = False,
        incl_edge_attributes: Union[bool, str, Iterable[str]] = False,
        omit_self_loops: bool = False,
    ) -> "nx.Graph":
        """Return the network data as a networkx graph object.

        Arguments:
            graph_type: the networkx Graph class to use
            incl_node_attributes: if True, all node attributes are included in the graph, if False, none are, otherwise the specified attributes are included
            incl_edge_attributes: if True, all edge attributes are included in the graph, if False, none are, otherwise the specified attributes are included
            omit_self_loops: if False, self-loops are included in the graph, otherwise they are not added to the resulting graph (nodes that are only connected to themselves are still included)

        """

        graph = graph_type()

        def add_node(node_id: int, *attrs):
            graph.add_node(node_id, *attrs)

        def add_edge(source: int, target: int, *attrs):
            graph.add_edge(source, target, *attrs)

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
    ) -> RUSTWORKX_GRAPH_TYPE:
        """
        Return the network data as a rustworkx graph object.

        Arguments:
            graph_type: the rustworkx Graph class to use
            multigraph: if True, a Multi(Di)Graph is returned, otherwise a normal (Di)Graph
            incl_node_attributes: if True, all node attributes are included in the graph, if False, none are, otherwise the specified attributes are included
            incl_edge_attributes: if True, all edge attributes are included in the graph, if False, none are, otherwise the specified attributes are included
            omit_self_loops: if False, self-loops are included in the graph, otherwise they are not added to the resulting graph (nodes that are only connected to themselves are still included)
        """

        graph = graph_type(multigraph=multigraph)

        self.get_number_of_nodes()
        # rustworkx uses 0-based integer indexes, so we don't neeed to look up the node ids (unless we want to
        # include node attributes)

        node_attr_names = self._calculate_node_attributes(incl_node_attributes)[1:]
        edge_attr_names = self._calculate_edge_attributes(incl_edge_attributes)[2:]

        def add_node(node_id: int, *attrs):
            data = {ID_COLUMN_NAME: node_id}
            if attrs:
                data.update(zip(node_attr_names, attrs))

            graph_node_id = graph.add_node(data)

            if not node_id == graph_node_id:
                raise Exception("Internal error: node ids don't match")

        def add_edge(source: int, target: int, *attrs):
            if not attrs:
                graph.add_edge(source, target, None)
            else:
                data = dict(zip(edge_attr_names, attrs))
                graph.add_edge(source, target, data)

        self.retrieve_graph_data(
            nodes_callback=add_node,
            edges_callback=add_edge,
            incl_node_attributes=incl_node_attributes,
            incl_edge_attributes=incl_edge_attributes,
            omit_self_loops=omit_self_loops,
        )

        return graph

    def _calculate_node_attributes(
        self, incl_node_attributes: Union[bool, str, Iterable[str]]
    ) -> List[str]:
        if incl_node_attributes is False:
            node_attr_names: List[str] = [ID_COLUMN_NAME]
        else:
            all_node_attr_names = self.nodes_schema.columns.keys()
            if incl_node_attributes is True:
                node_attr_names = [ID_COLUMN_NAME]
                node_attr_names.extend((x for x in all_node_attr_names if x != ID_COLUMN_NAME))  # type: ignore
            elif isinstance(incl_node_attributes, str):
                if incl_node_attributes not in all_node_attr_names:
                    raise KiaraException(
                        f"Can't include node attribute {incl_node_attributes}: not part of the available attributes ({', '.join(all_node_attr_names)})."
                    )
                node_attr_names = [ID_COLUMN_NAME, incl_node_attributes]
            else:
                node_attr_names = [ID_COLUMN_NAME]
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
        if incl_edge_attributes is False:
            edge_attr_names: List[str] = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
        else:
            all_edge_attr_names = self.edges_schema.columns.keys()
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
        nodes_callback: Union[Callable[[int, Tuple[Any, ...]], Any], None] = None,
        edges_callback: Union[Callable[[int, int, Tuple[Any, ...]], Any], None] = None,
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

        from sqlalchemy import select

        engine = self.get_sqlalchemy_engine()

        if nodes_callback is not None:
            node_attr_names = self._calculate_node_attributes(incl_node_attributes)

            with engine.connect() as conn:
                nodes = self.get_sqlalchemy_nodes_table()

                with conn.begin():
                    cols = []
                    for attr_name in node_attr_names:
                        cols.append(nodes.c[attr_name])
                    stmt = select(*cols)  # type: ignore
                    result = conn.execute(stmt)
                    for r in result:
                        nodes_callback(*r)

        if edges_callback is not None:
            edge_attr_names = self._calculate_edge_attributes(incl_edge_attributes)

            with engine.connect() as conn:
                edges = self.get_sqlalchemy_edges_table()

                cols = []
                for attr_name in edge_attr_names:
                    cols.append(edges.c[attr_name])
                stmt = select(*cols)  # type: ignore

                if omit_self_loops:
                    stmt = stmt.where(
                        edges.c[SOURCE_COLUMN_NAME] != edges.c[TARGET_COLUMN_NAME]
                    )

                result = conn.execute(stmt)
                for r in result:
                    edges_callback(*r)


class GraphType(Enum):
    """All possible graph types."""

    UNDIRECTED = "undirected"
    DIRECTED = "directed"
    UNDIRECTED_MULTI = "undirected-multi"
    DIRECTED_MULTI = "directed-multi"


class PropertiesByGraphType(BaseModel):
    """Properties of graph data, if interpreted as a specific graph type."""

    graph_type: GraphType = Field(description="The graph type name.")
    number_of_edges: int = Field(description="The number of edges.")


class NetworkGraphProperties(ValueMetadata):
    """File stats."""

    _metadata_key = "graph_properties"

    number_of_nodes: int = Field(description="Number of nodes in the network graph.")
    properties_by_graph_type: List[PropertiesByGraphType] = Field(
        description="Properties of the network data, by graph type."
    )
    number_of_self_loops: int = Field(
        description="Number of edges where source and target point to the same node."
    )
    number_of_parallel_edges: int = Field(description="Number of parallel edges.")

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["network_data"]

    @classmethod
    def create_value_metadata(cls, value: Value) -> "NetworkGraphProperties":
        from sqlalchemy import text

        network_data: NetworkData = value.data

        with network_data.get_sqlalchemy_engine().connect() as con:
            result = con.execute(
                text(f"SELECT count(*) from {NetworkDataTableType.NODES.value}")
            )
            num_rows = result.fetchone()[0]
            result = con.execute(
                text(f"SELECT count(*) from {NetworkDataTableType.EDGES.value}")
            )
            num_rows_eges = result.fetchone()[0]
            result = con.execute(
                text(
                    f"SELECT COUNT(*) FROM (SELECT DISTINCT {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME} FROM {NetworkDataTableType.EDGES.value})"
                )
            )
            num_edges_directed = result.fetchone()[0]
            query = f"SELECT COUNT(*) FROM {NetworkDataTableType.EDGES.value} WHERE rowid in (SELECT DISTINCT MIN(rowid) FROM (SELECT rowid, {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME} from {NetworkDataTableType.EDGES.value} UNION ALL SELECT rowid, {TARGET_COLUMN_NAME}, {SOURCE_COLUMN_NAME} from {NetworkDataTableType.EDGES.value}) GROUP BY {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME})"

            result = con.execute(text(query))
            num_edges_undirected = result.fetchone()[0]

            query = f"SELECT COUNT(*) FROM {NetworkDataTableType.EDGES.value} GROUP BY {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME} HAVING COUNT(*) > 1"
            result = con.execute(text(query))
            num_parallel_edges = 0
            for duplicates in result.fetchall():
                num_parallel_edges += duplicates[0] - 1

            query = f"SELECT count(*) FROM {NetworkDataTableType.EDGES.value} WHERE {SOURCE_COLUMN_NAME} = {TARGET_COLUMN_NAME}"
            result = con.execute(text(query))
            num_self_loops = result.fetchone()[0]

        directed = PropertiesByGraphType(
            graph_type=GraphType.DIRECTED, number_of_edges=num_edges_directed
        )
        undirected = PropertiesByGraphType(
            graph_type=GraphType.UNDIRECTED, number_of_edges=num_edges_undirected
        )
        directed_multi = PropertiesByGraphType(
            graph_type=GraphType.DIRECTED_MULTI, number_of_edges=num_rows_eges
        )
        undirected_multi = PropertiesByGraphType(
            graph_type=GraphType.UNDIRECTED_MULTI, number_of_edges=num_rows_eges
        )

        return cls(
            number_of_nodes=num_rows,
            properties_by_graph_type=[
                directed,
                undirected,
                directed_multi,
                undirected_multi,
            ],
            number_of_self_loops=num_self_loops,
            number_of_parallel_edges=num_parallel_edges,
        )
