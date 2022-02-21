# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_modules.network_analysis`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
import typing
from pathlib import Path

from kiara import KiaraEntryPointItem
from kiara.metadata import MetadataModel
from kiara.utils.class_loading import find_metadata_models_under
from kiara_modules.core.metadata_models import KiaraDatabase
from pydantic import Field, PrivateAttr

from kiara_modules.network_analysis.defaults import (
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    TEMPLATES_FOLDER,
    TableType,
)

if typing.TYPE_CHECKING:
    import networkx as nx
    from sqlalchemy import Metadata, Table  # noqa


metadata_models: KiaraEntryPointItem = (
    find_metadata_models_under,
    ["kiara_modules.network_analysis.metadata_models"],
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
    _metadata_obj = PrivateAttr(default=None)

    _nx_graph = PrivateAttr(default={})

    @classmethod
    def create_from_networkx_graph(
        cls, graph: "nx.Graph", edge_types: typing.Optional[typing.Mapping[str, str]]
    ) -> "NetworkData":

        # adapted from networx code
        # License: 3-clause BSD license
        # Copyright (C) 2004-2022, NetworkX Developers

        # edgelist = graph.edges(data=True)
        # source_nodes = [s for s, _, _ in edgelist]
        # target_nodes = [t for _, t, _ in edgelist]
        raise NotImplementedError()

        # all_attrs: typing.Set[str] = set().union(*(d.keys() for _, _, d in edgelist))
        #
        # if SOURCE_COLUMN_NAME in all_attrs:
        #     raise nx.NetworkXError(
        #         f"Source name {SOURCE_COLUMN_NAME} is an edge attr name"
        #     )
        # if SOURCE_COLUMN_NAME in all_attrs:
        #     raise nx.NetworkXError(
        #         f"Target name {SOURCE_COLUMN_NAME} is an edge attr name"
        #     )
        #
        # nan = float("nan")
        # edge_attr = {k: [d.get(k, nan) for _, _, d in edgelist] for k in all_attrs}
        #
        # edgelistdict = {
        #     SOURCE_COLUMN_NAME: source_nodes,
        #     TARGET_COLUMN_NAME: target_nodes,
        # }
        #
        # edgelistdict.update(edge_attr)

        # edge_list_df = nx.to_pandas_edgelist(
        #     graph, source=SOURCE_COLUMN_NAME, target=TARGET_COLUMN_NAME
        # )
        #
        # print(edge_list_df)
        # print(edge_list_df.dtypes["source"].__dict__)
        #
        # print("----")
        # network_data = cls.create_in_temp_dir()
        #
        # print(graph)
        # print("---")
        # nodes = graph.nodes
        #
        # network_data.insert_nodes(nodes)

    @classmethod
    def create_network_data_init_sql(
        cls,
        edge_attrs: typing.Optional[
            typing.Mapping[str, typing.Mapping[str, str]]
        ] = None,
        node_attrs: typing.Optional[
            typing.Mapping[str, typing.Mapping[str, str]]
        ] = None,
        id_type: typing.Optional[str] = None,
        extra_schema: typing.Optional[typing.Iterable[str]] = None,
        schema_template_str: typing.Optional[str] = None,
    ) -> str:
        """Create an sql script to initialize the edges and nodes tables.

        Arguments:
            edge_attrs: a map with the column name as key, and column details ('type', 'extra_column_info', 'create_index') as values
            node_attrs: a map with the column name as key, and column details ('type', 'extra_column_info', 'create_index') as values
            id_type: the type of the node 'id' column (as well as edge 'source' & 'target'), if 'None', this method will try to figure it out and fall back to 'TEXT' if it can't
        """

        if schema_template_str is None:
            template_path = Path(TEMPLATES_FOLDER) / "sqlite_schama.sql.j2"
            schema_template_str = template_path.read_text()

        edges: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        if edge_attrs:
            edges[SOURCE_COLUMN_NAME] = (
                {}
                if edge_attrs.get(SOURCE_COLUMN_NAME, None) is None
                else dict(edge_attrs[SOURCE_COLUMN_NAME])
            )
            edges[TARGET_COLUMN_NAME] = (
                {}
                if edge_attrs.get(TARGET_COLUMN_NAME, None) is None
                else dict(edge_attrs[TARGET_COLUMN_NAME])
            )
            for k, v in edge_attrs.items():
                if k in [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]:
                    continue
                edges[k] = dict(v)

        nodes: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        if node_attrs is not None:
            nodes[ID_COLUMN_NAME] = (
                {}
                if node_attrs.get(ID_COLUMN_NAME, None) is None
                else dict(node_attrs[ID_COLUMN_NAME])
            )
            nodes[LABEL_COLUMN_NAME] = (
                {}
                if node_attrs.get(LABEL_COLUMN_NAME, None) is None
                else dict(node_attrs[LABEL_COLUMN_NAME])
            )
            for k, v in node_attrs.items():
                if k in [ID_COLUMN_NAME, LABEL_COLUMN_NAME]:
                    continue
                nodes[k] = dict(v)

        if not id_type:
            _id_type = nodes.get(ID_COLUMN_NAME, {}).get("type", None)
            _source_type = edges.get(SOURCE_COLUMN_NAME, {}).get("type", None)
            _target_type = edges.get(TARGET_COLUMN_NAME, {}).get("type", None)

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
                id_type = "TEXT"
            else:
                id_type = _source_type

        edges.setdefault(SOURCE_COLUMN_NAME, {})["create_index"] = True
        edges[SOURCE_COLUMN_NAME]["type"] = id_type
        edges.setdefault(TARGET_COLUMN_NAME, {})["create_index"] = True
        edges[TARGET_COLUMN_NAME]["type"] = id_type

        FOREIGN_KEYS_STR = [
            f"    FOREIGN KEY({SOURCE_COLUMN_NAME}) REFERENCES nodes({ID_COLUMN_NAME})",
            f"    FOREIGN KEY({TARGET_COLUMN_NAME}) REFERENCES nodes({ID_COLUMN_NAME})",
        ]
        edge_sql = cls.create_table_init_sql(
            table_name=TableType.EDGES.value,
            column_attrs=edges,
            extra_schema=FOREIGN_KEYS_STR,
            schema_template_str=schema_template_str,
        )

        nodes.setdefault(ID_COLUMN_NAME, {})["create_index"] = True
        nodes[ID_COLUMN_NAME]["type"] = id_type
        if "extra_column_info" not in nodes[ID_COLUMN_NAME].keys():
            nodes[ID_COLUMN_NAME][
                "extra_column_info"
            ] = "NOT NULL UNIQUE"  # TODO: maybe also PRIMARY KEY?

        # TODO: check if already set to something else and fail?
        nodes.setdefault(LABEL_COLUMN_NAME, {})["type"] = "TEXT"

        node_sql = cls.create_table_init_sql(
            table_name=TableType.NODES.value,
            column_attrs=nodes,
            schema_template_str=schema_template_str,
        )

        if extra_schema is None:
            extra_schema = []
        else:
            extra_schema = list(extra_schema)

        extra_schema_str = "\n".join(extra_schema)

        init_sql = f"{edge_sql}\n{node_sql}\n{extra_schema_str}\n"
        return init_sql

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
