# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_modules.network_analysis`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
import typing
from enum import Enum

from kiara import KiaraEntryPointItem
from kiara.utils.class_loading import find_metadata_models_under
from kiara_modules.core.database import SqliteTableSchema, create_table_init_sql
from kiara_modules.core.defaults import DEFAULT_DB_CHUNK_SIZE
from kiara_modules.core.metadata_models import KiaraDatabase
from kiara_modules.core.table.utils import create_sqlite_schema_data_from_arrow_table
from pydantic import BaseModel, Field, PrivateAttr

from kiara_modules.network_analysis.defaults import (
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    TableType,
)
from kiara_modules.network_analysis.utils import (
    extract_edges_as_table,
    extract_nodes_as_table,
    insert_table_data_into_network_graph,
)

if typing.TYPE_CHECKING:
    import networkx as nx
    from sqlalchemy import Metadata, Table  # noqa


metadata_models: KiaraEntryPointItem = (
    find_metadata_models_under,
    ["kiara_modules.network_analysis.metadata_models"],
)


class GraphTypesEnum(Enum):

    undirected = "undirected"
    directed = "directed"
    multi_directed = "multi_directed"
    multi_undirected = "multi_undirected"


class NetworkDataSchema(BaseModel):

    edges_schema: typing.Optional[SqliteTableSchema] = Field(
        description="The schema information for the edges table."
    )
    nodes_schema: typing.Optional[SqliteTableSchema] = Field(
        description="The schema information for the nodes table."
    )

    id_type: typing.Optional[str] = Field(
        description="The type of the node 'id' column (as well as edge 'source' & 'target'), if 'None', this method will try to figure it out and fall back to 'TEXT' if it can't.",
        default=None,
    )
    extra_schema: typing.List[str] = Field(
        description="Any extra schema creation code that should be appended to the created sql script.",
        default_factory=list,
    )

    _edges_schema_final = PrivateAttr(default=None)
    _nodes_schema_final = PrivateAttr(default=None)
    _id_type_final = PrivateAttr(default=None)

    def _calculate_final_schemas(self):
        """Utility method to calculate the final schema, that will adhere to what the NetworkData class expects to find."""

        edges: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        if self.edges_schema:
            edges[SOURCE_COLUMN_NAME] = (
                {}
                if self.edges_schema.columns.get(SOURCE_COLUMN_NAME, None) is None
                else dict(self.edges_schema.columns[SOURCE_COLUMN_NAME])
            )
            edges[TARGET_COLUMN_NAME] = (
                {}
                if self.edges_schema.columns.get(TARGET_COLUMN_NAME, None) is None
                else dict(self.edges_schema.columns[TARGET_COLUMN_NAME])
            )
            for k, v in self.edges_schema.columns.items():
                if k in [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]:
                    continue
                edges[k] = dict(v)
        else:
            if self.edges_schema.extra_schema:
                raise NotImplementedError()

        nodes: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        if self.nodes_schema is not None:
            if self.nodes_schema.extra_schema:
                raise NotImplementedError()

            nodes[ID_COLUMN_NAME] = (
                {}
                if self.nodes_schema.columns.get(ID_COLUMN_NAME, None) is None
                else dict(self.nodes_schema.columns[ID_COLUMN_NAME])
            )
            nodes[LABEL_COLUMN_NAME] = (
                {}
                if self.nodes_schema.columns.get(LABEL_COLUMN_NAME, None) is None
                else dict(self.nodes_schema.columns[LABEL_COLUMN_NAME])
            )
            for k, v in self.nodes_schema.columns.items():
                if k in [ID_COLUMN_NAME, LABEL_COLUMN_NAME]:
                    continue
                nodes[k] = dict(v)

        if not self.id_type:
            _id_type = nodes.get(ID_COLUMN_NAME, {}).get("data_type", None)
            _source_type = edges.get(SOURCE_COLUMN_NAME, {}).get("data_type", None)
            _target_type = edges.get(TARGET_COLUMN_NAME, {}).get("data_type", None)

            if _source_type is None:
                if _target_type:
                    _source_type = _target_type
            if _target_type is None:
                if _source_type:
                    _target_type = _source_type

            if _source_type != _target_type:
                raise Exception(
                    f"Can't create network data init sql, source and target column type for edges table are not the same: {_source_type} <-> {_target_type}"
                )

            if _id_type is None:
                _id_type = _source_type
            elif _source_type is None:
                _source_type = _id_type
            elif _id_type != _source_type:
                raise Exception(
                    f"Can't create network data init sql, edge and node id types are not the same: {_source_type} <-> {_id_type}"
                )

            if _source_type is None:
                id_type_final = "TEXT"
            else:
                id_type_final = _source_type
        else:
            id_type_final = self.id_type

        edges.setdefault(SOURCE_COLUMN_NAME, {})["create_index"] = True
        edges[SOURCE_COLUMN_NAME]["data_type"] = id_type_final
        edges.setdefault(TARGET_COLUMN_NAME, {})["create_index"] = True
        edges[TARGET_COLUMN_NAME]["data_type"] = id_type_final

        FOREIGN_KEYS_STR = [
            f"    FOREIGN KEY({SOURCE_COLUMN_NAME}) REFERENCES nodes({ID_COLUMN_NAME})",
            f"    FOREIGN KEY({TARGET_COLUMN_NAME}) REFERENCES nodes({ID_COLUMN_NAME})",
        ]
        edges_schema_final = SqliteTableSchema(
            columns=edges, extra_schema=FOREIGN_KEYS_STR
        )

        nodes.setdefault(ID_COLUMN_NAME, {})["create_index"] = True
        nodes[ID_COLUMN_NAME]["data_type"] = id_type_final
        if "extra_column_info" not in nodes[ID_COLUMN_NAME].keys():
            nodes[ID_COLUMN_NAME]["extra_column_info"] = [
                "NOT NULL",
                "UNIQUE",
            ]  # TODO: maybe also PRIMARY KEY?

        # TODO: check if already set to something else and fail?
        nodes.setdefault(LABEL_COLUMN_NAME, {})["data_type"] = "TEXT"

        nodes_schema_final = SqliteTableSchema(columns=nodes)

        self._edges_schema_final = edges_schema_final
        self._nodes_schema_final = nodes_schema_final
        self._id_type_final = id_type_final

    def create_edges_init_sql(self, schema_template_str: typing.Optional[str] = None):

        edges_sql = create_table_init_sql(
            table_name=TableType.EDGES.value,
            table_schema=self.edges_schema_final,
            schema_template_str=schema_template_str,
        )
        return edges_sql

    def create_nodes_init_sql(self, schema_template_str: typing.Optional[str] = None):

        nodes_sql = create_table_init_sql(
            table_name=TableType.NODES.value,
            table_schema=self.nodes_schema_final,
            schema_template_str=schema_template_str,
        )
        return nodes_sql

    def create_init_sql(self) -> str:

        if self.extra_schema is None:
            extra_schema = []
        else:
            extra_schema = list(self.extra_schema)

        extra_schema_str = "\n".join(extra_schema)

        init_sql = f"{self.create_nodes_init_sql()}\n{self.create_edges_init_sql()}\n{extra_schema_str}\n"
        return init_sql

    @property
    def edges_schema_final(self):
        if self._edges_schema_final is None:
            self._calculate_final_schemas()
        return self._edges_schema_final  # type: ignore

    @property
    def nodes_schema_final(self):
        if self._nodes_schema_final is None:
            self._calculate_final_schemas()
        return self._nodes_schema_final  # type: ignore

    @property
    def id_type_final(self):
        if self._id_type_final is None:
            self._calculate_final_schemas()
        return self._id_type_final  # type: ignore

    def invalidate(self):

        self._nodes_schema_final = None
        self._edges_schema_final = None
        self._id_type_final = None


class NetworkData(KiaraDatabase):

    _metadata_key: typing.ClassVar[str] = "network_data"

    _nodes_table_obj = PrivateAttr(default=None)
    _edges_table_obj = PrivateAttr(default=None)
    _metadata_obj = PrivateAttr(default=None)

    _nx_graph = PrivateAttr(default={})

    @classmethod
    def create_from_networkx_graph(cls, graph: "nx.Graph") -> "NetworkData":

        edges_table = extract_edges_as_table(graph)
        edges_schema = create_sqlite_schema_data_from_arrow_table(edges_table)

        nodes_table = extract_nodes_as_table(graph)
        nodes_schema = create_sqlite_schema_data_from_arrow_table(nodes_table)

        nd_schema = NetworkDataSchema(
            edges_schema=edges_schema, nodes_schema=nodes_schema
        )
        init_sql = nd_schema.create_init_sql()

        network_data = NetworkData.create_in_temp_dir(init_sql=init_sql)
        insert_table_data_into_network_graph(
            network_data=network_data,
            edges_table=edges_table,
            edges_schema=edges_schema,
            nodes_table=nodes_table,
            nodes_schema=nodes_schema,
            chunk_size=DEFAULT_DB_CHUNK_SIZE,
        )

        return network_data

    def get_sqlalchemy_metadata(self) -> "Metadata":

        if self._metadata_obj is None:
            from sqlalchemy import MetaData

            self._metadata_obj = MetaData()
        return self._metadata_obj

    def get_sqlalchemy_nodes_table(self) -> "Table":

        if self._nodes_table_obj is not None:
            return self._nodes_table_obj

        from sqlalchemy import Table

        self._nodes_table_obj = Table(
            TableType.NODES.value,
            self.get_sqlalchemy_metadata(),
            autoload_with=self.get_sqlalchemy_engine(),
        )
        return self._nodes_table_obj

    def get_sqlalchemy_edges_table(self) -> "Table":

        if self._edges_table_obj is not None:
            return self._edges_table_obj

        from sqlalchemy import Table

        self._edges_table_obj = Table(
            TableType.EDGES.value,
            self.get_sqlalchemy_metadata(),
            autoload_with=self.get_sqlalchemy_engine(),
        )
        return self._edges_table_obj

    def insert_nodes(self, *nodes: typing.Mapping[str, typing.Any]):
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
