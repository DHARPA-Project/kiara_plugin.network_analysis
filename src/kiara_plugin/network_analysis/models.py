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
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Set, Type, Union

from kiara_plugin.tabular.models.db import KiaraDatabase, SqliteTableSchema
from kiara_plugin.tabular.utils import create_sqlite_schema_data_from_arrow_table
from pydantic import Field, PrivateAttr, root_validator
from sqlalchemy import MetaData, Table

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

    @classmethod
    def create_from_networkx_graph(cls, graph: "nx.Graph") -> "NetworkData":
        """Create a `NetworkData` instance from a networkx Graph object."""

        edges_table = extract_edges_as_table(graph)
        edges_schema = create_sqlite_schema_data_from_arrow_table(edges_table)

        nodes_table = extract_nodes_as_table(graph)
        nodes_schema = create_sqlite_schema_data_from_arrow_table(nodes_table)

        network_data = NetworkData.create_in_temp_dir(
            edges_schema=edges_schema, nodes_schema=nodes_schema, keep_unlocked=True
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
    def create_in_temp_dir(
        cls,
        edges_schema: Union[None, SqliteTableSchema, Mapping] = None,
        nodes_schema: Union[None, SqliteTableSchema, Mapping] = None,
        keep_unlocked: bool = False,
    ):

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "network_data.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

        atexit.register(cleanup)

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

    @root_validator(pre=True)
    def pre_validate(cls, values):

        _edges_schema = values.get("edges_schema", None)
        _nodes_schema = values.get("nodes_schema", None)
        if _edges_schema is None:

            suggested_id_type = "TEXT"
            if _nodes_schema is not None:
                if isinstance(_nodes_schema, Mapping):
                    suggested_id_type = _nodes_schema.get(ID_COLUMN_NAME, "TEXT")
                elif isinstance(_nodes_schema, SqliteTableSchema):
                    suggested_id_type = _nodes_schema.columns.get(
                        ID_COLUMN_NAME, "TEXT"
                    )

            edges_schema = SqliteTableSchema.construct(
                columns={
                    SOURCE_COLUMN_NAME: suggested_id_type,
                    TARGET_COLUMN_NAME: suggested_id_type,
                }
            )
        else:
            if isinstance(_edges_schema, Mapping):
                edges_schema = SqliteTableSchema(**_edges_schema)
            elif not isinstance(_edges_schema, SqliteTableSchema):
                raise ValueError(
                    f"Invalid data type for edges schema: {type(_edges_schema)}"
                )
            else:
                edges_schema = _edges_schema

        if (
            edges_schema.columns[SOURCE_COLUMN_NAME]
            != edges_schema.columns[TARGET_COLUMN_NAME]
        ):
            raise ValueError(
                f"Invalid edges schema, source and edges columns have different type: {edges_schema[SOURCE_COLUMN_NAME]} != {edges_schema[TARGET_COLUMN_NAME]}"
            )

        if _nodes_schema is None:

            _nodes_schema = SqliteTableSchema.construct(
                columns={
                    ID_COLUMN_NAME: edges_schema.columns[SOURCE_COLUMN_NAME],
                    LABEL_COLUMN_NAME: "TEXT",
                }
            )

        if isinstance(_nodes_schema, Mapping):
            nodes_schema = SqliteTableSchema(**_nodes_schema)
        elif isinstance(_nodes_schema, SqliteTableSchema):
            nodes_schema = _nodes_schema
        else:
            raise ValueError(
                f"Invalid data type for nodes schema: {type(_edges_schema)}"
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

        values["edges_schema"] = edges_schema
        values["nodes_schema"] = nodes_schema

        return values

    _nodes_table_obj: Optional[Table] = PrivateAttr(default=None)
    _edges_table_obj: Optional[Table] = PrivateAttr(default=None)
    _metadata_obj: Optional[MetaData] = PrivateAttr(default=None)

    _nx_graph = PrivateAttr(default={})

    def _invalidate_other(self):

        self._metadata_obj = None
        self._nodes_table_obj = None
        self._edges_table_obj = None

    def get_sqlalchemy_metadata(self) -> MetaData:
        """Return the sqlalchemy Metadtaa object for the underlying database.

        This is used internally, you typically don't need to access this attribute.

        """

        if self._metadata_obj is None:
            self._metadata_obj = MetaData()
        return self._metadata_obj

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
        existing_node_ids: Iterable[int] = None,
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
