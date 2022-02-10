# -*- coding: utf-8 -*-

"""This module contains the metadata models that are used in the ``kiara_modules.network_analysis`` package.

Metadata models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata that
is attached to data, as well as *kiara* modules. It is possible to register metadata using a JSON schema string, but
it is recommended to create a metadata model, because it is much easier overall.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel].
"""
import atexit
import json
import os
import shutil
import tempfile
import typing

from kiara import KiaraEntryPointItem
from kiara.metadata import MetadataModel
from kiara.utils.class_loading import find_metadata_schemas_under
from kiara_modules.core.metadata_schemas import KiaraDatabase
from pydantic import Field, PrivateAttr
from simple_graph_sqlite import database as simple_graph_db
from simple_graph_sqlite.database import read_sql

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

    @classmethod
    def create_in_temp_dir(cls):

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "db.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

        atexit.register(cleanup)

        db = NetworkData(db_file_path=db_path)
        db.initialize()
        return db

    _invalidated: bool = PrivateAttr(default=False)

    def initialize(self):

        simple_graph_db.initialize(self.db_file_path)

    def upsert_nodes(
        self,
        node_ids: typing.List[int],
        node_details: typing.Iterable[typing.Mapping[str, typing.Any]],
    ):

        # if len(node_ids) != len(node_details):
        #     raise Exception("Can't add nodes: different length for node_ids and node_details.")

        def _prepare_item(identifier, data):
            if identifier is None:
                raise Exception("Missing identifier.")

            if data is None:
                data = {}
            if "id" not in data.keys():
                data["id"] = identifier
            elif data["id"] != identifier:
                raise Exception(f"Clashing ids: {data['id']} <-> {identifier}")
            return data

        def _add_nodes(cursor):
            cursor.executemany(
                read_sql("insert-node.sql"),
                [
                    (x,)
                    for x in map(
                        lambda node: json.dumps(_prepare_item(node[0], node[1])),
                        zip(node_ids, node_details),
                    )
                ],
            )

        simple_graph_db.atomic(self.db_file_path, _add_nodes)
        self._invalidated = True

    def insert_edges(
        self,
        edges_dict: typing.Mapping[str, typing.Sequence[typing.Any]],
        source_column_name: str = "source",
        target_column_name: str = "target",
        existing_node_ids: typing.Set[int] = None,
    ) -> typing.Set[int]:
        """Add edges to a network data item.

        All the edges need to have their node-ids registered already.

        Returns:
            a unique set of all node ids contained in source and target columns
        """

        if existing_node_ids is None:
            existing_node_ids = set()

        required_node_ids = set(edges_dict[source_column_name])
        required_node_ids.update(edges_dict[target_column_name])

        node_ids = list(required_node_ids.difference(existing_node_ids))

        if node_ids:
            self.upsert_nodes(
                node_ids=node_ids,
                node_details=({"id": i, "label": str(i)} for i in node_ids),
            )

        def get_edge_data(index: int):

            source_edge = None
            target_edge = None
            other = {}

            for key in edges_dict.keys():
                if key == source_column_name:
                    source_edge = edges_dict[key][index]
                elif key == target_column_name:
                    target_edge = edges_dict[key][index]
                else:
                    other[key] = edges_dict[key][index]

            return (source_edge, target_edge, json.dumps(other))

        def _add_edges(cursor):
            cursor.executemany(
                read_sql("insert-edge.sql"),
                [
                    get_edge_data(index)
                    for index in range(0, len(edges_dict[source_column_name]))
                ],
            )

        simple_graph_db.atomic(self.db_file_path, _add_edges)
        self._invalidated = True

        return required_node_ids

    # def get_simple_graph_db(self):
    #
    #     if self._simple_graph_db is not None:
    #         return self._simple_graph_db
    #
    #     from simple_graph_sqlite import database as simple_graph_db
    #     self._simple_graph_db = simple_graph_db.initialize(self.db_file_path)
    #     return self._simple_graph_db
