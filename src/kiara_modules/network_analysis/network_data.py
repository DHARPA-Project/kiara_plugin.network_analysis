# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import typing
from enum import Enum

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.operations.create_value import CreateValueModule
from kiara.operations.extract_metadata import ExtractMetadataModule
from kiara_modules.core.metadata_schemas import KiaraFile
from kiara_modules.core.table.utils import create_sqlite_schema_data_from_arrow_table
from pydantic import BaseModel, Field

from kiara_modules.network_analysis.defaults import (
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_modules.network_analysis.metadata_schemas import NetworkData

if typing.TYPE_CHECKING:
    import pyarrow as pa


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


class NetworkProperties(BaseModel):
    """Common properties of network data."""

    number_of_nodes: int = Field(description="Number of nodes in the network graph.")
    properties_by_graph_type: typing.List[PropertiesByGraphType] = Field(
        description="Properties of the network data, by graph type."
    )


class ExtractNetworkPropertiesMetadataModule(ExtractMetadataModule):
    """Extract commpon properties of network data."""

    _module_type_name = "network_properties"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "network_data"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        """Create the metadata schema for the configured type."""

        return NetworkProperties

    def extract_metadata(
        self, value: Value
    ) -> typing.Union[typing.Mapping[str, typing.Any], BaseModel]:

        from sqlalchemy import text

        network_data: NetworkData = value.get_value_data()

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

        return NetworkProperties(
            number_of_nodes=num_rows,
            properties_by_graph_type=[
                directed,
                undirected,
                directed_multi,
                undirected_multi,
            ],
        )


class CreateGraphFromTablesModule(KiaraModule):
    """Create a graph object from a file."""

    _module_type_name = "from_tables"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "edges": {
                "type": "table",
                "doc": "A table that contains the edges data.",
                "optional": False,
            },
            "edges_source_column_name": {
                "type": "string",
                "doc": "The name of the source column name in the edges table.",
                "default": "source",
            },
            "edges_target_column_name": {
                "type": "string",
                "doc": "The name of the target column name in the edges table.",
                "default": "target",
            },
            "edges_column_map": {
                "type": "dict",
                "doc": "An optional map of original column name to desired.",
                "optional": True,
            },
            "nodes": {
                "type": "table",
                "doc": "A table that contains the nodes data.",
                "optional": True,
            },
            "id_column_name": {
                "type": "string",
                "doc": "The name (before any potential column mapping) of the node-table column that contains the node identifier (used in the edges table).",
                "default": "id",
            },
            "label_column_name": {
                "type": "string",
                "doc": "The name of a column that contains the node label (before any potential column name mapping). If not specified, the value of the id value will be used as label.",
                "optional": True,
            },
            "nodes_column_map": {
                "type": "dict",
                "doc": "An optional map of original column name to desired.",
                "optional": True,
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "network_data": {"type": "network_data", "doc": "The network/graph data."}
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        edges = inputs.get_value_obj("edges")
        edges_source_column_name = inputs.get_value_data("edges_source_column_name")
        edges_target_column_name = inputs.get_value_data("edges_target_column_name")

        edges_columns = edges.get_metadata("table")["table"]["column_names"]
        if edges_source_column_name not in edges_columns:
            raise KiaraProcessingException(
                f"Edges table does not contain source column '{edges_source_column_name}'. Choose one of: {', '.join(edges_columns)}."
            )
        if edges_target_column_name not in edges_columns:
            raise KiaraProcessingException(
                f"Edges table does not contain target column '{edges_source_column_name}'. Choose one of: {', '.join(edges_columns)}."
            )

        nodes = inputs.get_value_obj("nodes")

        DEFAULT_DB_CHUNK_SIZE = 1024

        added_node_ids = set()
        edges_table: pa.Table = edges.get_value_data()

        id_column_name = inputs.get_value_data("id_column_name")
        label_column_name = inputs.get_value_data("label_column_name")
        nodes_column_map: typing.Dict[str, str] = inputs.get_value_data(
            "nodes_column_map"
        )
        if nodes_column_map is None:
            nodes_column_map = {}

        edges_column_map: typing.Dict[str, str] = inputs.get_value_data(
            "edges_column_map"
        )
        if edges_column_map is None:
            edges_column_map = {}
        if edges_source_column_name in edges_column_map.keys():
            raise KiaraProcessingException(
                "The value of the 'source_column_name' argument is not allowed in the edges column map."
            )
        if edges_target_column_name in edges_column_map.keys():
            raise KiaraProcessingException(
                "The value of the 'source_column_name' argument is not allowed in the edges column map."
            )

        edges_column_map[edges_source_column_name] = SOURCE_COLUMN_NAME
        edges_column_map[edges_target_column_name] = TARGET_COLUMN_NAME

        edges_data = create_sqlite_schema_data_from_arrow_table(
            index_columns=[SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME],
            column_map=edges_column_map,
        )

        nodes_table: typing.Optional[pa.Table] = None
        if nodes.is_set:
            if id_column_name in nodes_column_map.keys():
                raise KiaraProcessingException(
                    "The value of the 'id_column_name' argument is not allowed in the node column map."
                )

            nodes_column_map[id_column_name] = ID_COLUMN_NAME

            nodes_table = nodes.get_value_data()

            extra_schema = []
            if label_column_name is None:
                label_column_name = LABEL_COLUMN_NAME

            for cn in nodes_table.column_names:
                if cn.lower() == LABEL_COLUMN_NAME.lower():
                    label_column_name = cn
                    break

            if LABEL_COLUMN_NAME in nodes_table.column_names:
                if label_column_name != LABEL_COLUMN_NAME:
                    raise KiaraProcessingException(
                        f"Can't create database for graph data: original data contains column called 'label', which is a protected column name. If this column can be used as a label, remove your '{label_column_name}' input value for the 'label_column_name' input and re-run this module."
                    )

            if label_column_name in nodes_table.column_names:
                if label_column_name in nodes_column_map.keys():
                    raise KiaraProcessingException(
                        "The value of the 'label_column_name' argument is not allowed in the node column map."
                    )
            else:
                extra_schema.append("    label    TEXT")

            nodes_column_map[label_column_name] = LABEL_COLUMN_NAME

            nodes_data = create_sqlite_schema_data_from_arrow_table(
                table=nodes_table,
                index_columns=[ID_COLUMN_NAME],
                column_map=nodes_column_map,
                extra_column_info={ID_COLUMN_NAME: "NOT NULL UNIQUE"},
            )

        else:
            nodes_data = None

        init_sql = NetworkData.create_network_data_init_sql(
            edge_attrs=edges_data, node_attrs=nodes_data
        )
        network_data = NetworkData.create_in_temp_dir(init_sql=init_sql)

        # =============================================
        # import data

        if nodes_table is not None:
            for batch in nodes_table.to_batches(DEFAULT_DB_CHUNK_SIZE):
                batch_dict = batch.to_pydict()

                for k, v in nodes_column_map.items():
                    if k in batch_dict.keys():
                        if k == ID_COLUMN_NAME and v == LABEL_COLUMN_NAME:
                            _data = batch_dict.get(k)
                        else:
                            _data = batch_dict.pop(k)
                            if v in batch_dict.keys():
                                raise Exception(
                                    "Duplicate nodes column name after mapping: {v}"
                                )
                        batch_dict[v] = _data

                ids = batch_dict[ID_COLUMN_NAME]
                data = [dict(zip(batch_dict, t)) for t in zip(*batch_dict.values())]
                network_data.insert_nodes(*data)

                added_node_ids.update(ids)

        for batch in edges_table.to_batches(DEFAULT_DB_CHUNK_SIZE):

            batch_dict = batch.to_pydict()
            for k, v in edges_column_map.items():
                if k in batch_dict.keys():
                    _data = batch_dict.pop(k)
                    if v in batch_dict.keys():
                        raise Exception(
                            "Duplicate edges column name after mapping: {v}"
                        )
                    batch_dict[v] = _data

            data = [dict(zip(batch_dict, t)) for t in zip(*batch_dict.values())]

            all_node_ids = network_data.insert_edges(
                *data,
                existing_node_ids=added_node_ids,
            )
            added_node_ids.update(all_node_ids)

        outputs.set_value("network_data", network_data)


class CreateNetworkDataModule(CreateValueModule):
    @classmethod
    def get_target_value_type(cls) -> str:
        return "network_data"

    def from_graphml_file(self, value: Value):

        input_file: KiaraFile = value.get_value_data()

        from kiara_modules.network_analysis.utils import parse_graphml_file

        graph, edge_props, node_props = parse_graphml_file(input_file.path)

        label_match: typing.Optional[str] = None
        for column_name in node_props.keys():
            if column_name.lower() == LABEL_COLUMN_NAME.lower():
                label_match = column_name
                break

        if label_match:
            temp = node_props.pop(label_match)
            node_props[LABEL_COLUMN_NAME] = temp
        else:
            node_props[LABEL_COLUMN_NAME] = {"type": "TEXT"}

        init_sql = NetworkData.create_network_data_init_sql(
            edge_attrs=edge_props, node_attrs=node_props
        )
        network_data = NetworkData.create_in_temp_dir(init_sql=init_sql)

        nodes = []
        node_ids = []
        for node in graph.nodes():
            data = {}
            for v in node.attr.values():
                if label_match and label_match == v.name:
                    data[LABEL_COLUMN_NAME] = v.value
                else:
                    data[v.name] = v.value
            if LABEL_COLUMN_NAME not in data.keys() or not data[LABEL_COLUMN_NAME]:
                data[LABEL_COLUMN_NAME] = str(node.id)
            data[ID_COLUMN_NAME] = node.id
            node_ids.append(node.id)
            nodes.append(data)

        network_data.insert_nodes(*nodes)

        edges = []
        for edge in graph.edges():
            data = {}
            for v in edge.attr.values():
                data[v.name] = v.value

            data[SOURCE_COLUMN_NAME] = edge.node1.id
            data[TARGET_COLUMN_NAME] = edge.node2.id
            edges.append(data)

        network_data.insert_edges(*edges, existing_node_ids=node_ids)

        return network_data

        # graph = nx.read_graphml(input_file.path)
        # network_data = NetworkData.create_from_networkx_graph(graph=graph)
        #
        # print("XXXXXXXXXXXXX")
