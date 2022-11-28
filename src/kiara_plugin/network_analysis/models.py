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
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Set, Type, Union

from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
from kiara_plugin.tabular.defaults import SqliteDataType
from kiara_plugin.tabular.models.db import KiaraDatabase, SqliteTableSchema
from kiara_plugin.tabular.utils import create_sqlite_schema_data_from_arrow_table
from pydantic import BaseModel, Field, PrivateAttr
from sqlalchemy import Table

from kiara_plugin.network_analysis.defaults import (
    DEFAULT_NETWORK_DATA_CHUNK_SIZE,
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    NetworkDataTableType,
)
from kiara_plugin.network_analysis.utils import (
    extract_edges_as_table,
    extract_nodes_as_table,
    insert_table_data_into_network_graph,
)

if TYPE_CHECKING:
    import networkx as nx


class GraphTypesEnum(Enum):

    undirected = "undirected"
    directed = "directed"
    multi_directed = "multi_directed"
    multi_undirected = "multi_undirected"


class NetworkData(KiaraDatabase):
    """A helper class to access and query network datasets.

    This class provides different ways to access the underlying network data, most notably via sql and as networkx Graph object.

    Internally, network data is stored in a sqlite database with the edges stored in a table called 'edges' and the nodes, well,
    in a table aptly called 'nodes'.

    """

    _kiara_model_id = "instance.network_data"

    @classmethod
    def create_from_networkx_graph(cls, graph: "nx.Graph") -> "NetworkData":
        """Create a `NetworkData` instance from a networkx Graph object."""

        edges_table = extract_edges_as_table(graph)
        edges_schema = create_sqlite_schema_data_from_arrow_table(edges_table)

        nodes_table = extract_nodes_as_table(graph)
        nodes_schema = create_sqlite_schema_data_from_arrow_table(nodes_table)

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

        return cls.create_network_data_from_database(
            KiaraDatabase(db_file_path=db_file_path)
        )

    @classmethod
    def create_network_data_from_database(cls, db: KiaraDatabase) -> "NetworkData":

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

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "network_data.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

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
        else:
            if isinstance(schema_edges, Mapping):
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
        else:
            if nodes_schema.columns[LABEL_COLUMN_NAME] != "TEXT":
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

    # @root_validator(pre=True)
    # def pre_validate(cls, values):
    #
    #     _edges_schema = values.get("edges_schema", None)
    #     _nodes_schema = values.get("nodes_schema", None)
    #     _path = values.get("db_file_path", None)
    #     if _path is not None:
    #         db = KiaraDatabase(db_file_path=_path)
    #         if _edges_schema is not None:
    #             raise ValueError(
    #                 "Can't initialize network data with both 'db_file_path' and 'edges_schema'."
    #             )
    #         if _nodes_schema is not None:
    #             raise ValueError(
    #                 "Can't initialize network data with both 'db_file_path' and 'nodes_schema'."
    #             )
    #
    #         md = db.create_metadata()
    #         edges_col_schema = md.tables.get(
    #             NetworkDataTableType.EDGES.value
    #         ).column_schema
    #         nodes_col_schema = md.tables.get(
    #             NetworkDataTableType.NODES.value
    #         ).column_schema
    #         edges_schema = SqliteTableSchema(**edges_col_schema)
    #         nodes_schema = SqliteTableSchema(**nodes_col_schema)
    #
    #         values["edges_schema"] = edges_schema
    #         values["nodes_schema"] = nodes_schema
    #
    #     return values

    _nodes_table_obj: Union[Table, None] = PrivateAttr(default=None)
    _edges_table_obj: Union[Table, None] = PrivateAttr(default=None)

    _nx_graph = PrivateAttr(default={})

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

        required_node_ids = set((edge[SOURCE_COLUMN_NAME] for edge in edges))
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

    def as_networkx_graph(self, graph_type: Type["nx.Graph"]) -> "nx.Graph":
        """Return the network data as a networkx graph object.

        Arguments:
            graph_type: the networkx Graph class to use
        """

        if graph_type in self._nx_graph.keys():
            return self._nx_graph[graph_type]

        graph = graph_type()

        engine = self.get_sqlalchemy_engine()
        nodes = self.get_sqlalchemy_nodes_table()
        edges = self.get_sqlalchemy_edges_table()

        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(nodes.select())
                for r in result:
                    row = dict(r)
                    node_id = row.pop(ID_COLUMN_NAME)
                    graph.add_node(node_id, **row)

                result = conn.execute(edges.select())
                for r in result:
                    row = dict(r)
                    source = row.pop(SOURCE_COLUMN_NAME)
                    target = row.pop(TARGET_COLUMN_NAME)
                    graph.add_edge(source, target, **row)

        self._nx_graph[graph_type] = graph
        return self._nx_graph[graph_type]


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

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["network_data"]

    @classmethod
    def create_value_metadata(cls, value: Value) -> "NetworkGraphProperties":

        from sqlalchemy import text

        network_data: NetworkData = value.data

        with network_data.get_sqlalchemy_engine().connect() as con:
            result = con.execute(text("SELECT count(*) from nodes"))
            num_rows = result.fetchone()[0]
            result = con.execute(text("SELECT count(*) from edges"))
            num_rows_eges = result.fetchone()[0]
            result = con.execute(
                text("SELECT COUNT(*) FROM (SELECT DISTINCT source, target FROM edges)")
            )
            num_edges_directed = result.fetchone()[0]
            query = "SELECT COUNT(*) FROM edges WHERE rowid in (SELECT DISTINCT MIN(rowid) FROM (SELECT rowid, source, target from edges UNION ALL SELECT rowid, target, source from edges) GROUP BY source, target)"

            result = con.execute(text(query))
            num_edges_undirected = result.fetchone()[0]

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
        )
