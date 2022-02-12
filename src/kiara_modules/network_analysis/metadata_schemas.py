# -*- coding: utf-8 -*-

"""This module contains the metadata models that are used in the ``kiara_modules.network_analysis`` package.

Metadata models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata that
is attached to data, as well as *kiara* modules. It is possible to register metadata using a JSON schema string, but
it is recommended to create a metadata model, because it is much easier overall.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel].
"""
import atexit
import os
import shutil
import tempfile
import typing

from kiara import KiaraEntryPointItem
from kiara.metadata import MetadataModel
from kiara.utils.class_loading import find_metadata_schemas_under
from kiara_modules.core.metadata_schemas import KiaraDatabase
from pydantic import Field, PrivateAttr
from sqlalchemy import MetaData, Table

from kiara_modules.network_analysis.defaults import (
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    TableType,
)

if typing.TYPE_CHECKING:
    import networkx as nx

metadata_schemas: KiaraEntryPointItem = (
    find_metadata_schemas_under,
    ["kiara_modules.network_analysis.metadata_schemas"],
)


class GraphMetadata(MetadataModel):

    number_of_nodes: int = Field(description="The number of nodes in this graph.")
    number_of_edges: int = Field(description="The number of edges in this graph.")
    directed: bool = Field(description="Whether the graph is directed or not.")
    density: float = Field(description="The density of the graph.")


class NetworkData(KiaraDatabase):

    _metadata_key: typing.ClassVar[str] = "network_data"

    _nodes_table_obj = PrivateAttr(default=None)
    _edges_table_obj = PrivateAttr(default=None)
    _metadata_obj = PrivateAttr(default=MetaData())

    _nx_graph = PrivateAttr(default={})

    @classmethod
    def create_in_temp_dir(cls):

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "db.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

        atexit.register(cleanup)

        db = NetworkData(db_file_path=db_path)
        return db

    def get_sqlalchemy_nodes_table(self) -> Table:

        if self._nodes_table_obj is not None:
            return self._nodes_table_obj

        self._nodes_table_obj = Table(
            TableType.NODES.value,
            self._metadata_obj,
            autoload_with=self.get_sqlalchemy_engine(),
        )
        return self._nodes_table_obj

    def get_sqlalchemy_edges_table(self) -> Table:

        if self._edges_table_obj is not None:
            return self._edges_table_obj

        self._edges_table_obj = Table(
            TableType.EDGES.value,
            self._metadata_obj,
            autoload_with=self.get_sqlalchemy_engine(),
        )
        return self._edges_table_obj

    def insert_nodes(self, *nodes: typing.Mapping[str, typing.Any]):

        engine = self.get_sqlalchemy_engine()
        nodes_table = self.get_sqlalchemy_nodes_table()

        with engine.connect() as conn:
            with conn.begin():
                conn.execute(nodes_table.insert(), nodes)

    def insert_edges(
        self,
        *edges: typing.Mapping[str, typing.Any],
        existing_node_ids: typing.Iterable[int] = None,
    ) -> typing.Set[int]:
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

    def as_networkx_graph(self, graph_type: typing.Type["nx.Graph"]) -> "nx.Graph":

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
