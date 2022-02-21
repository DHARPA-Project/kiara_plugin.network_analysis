# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import csv
import os
import shutil
import typing
from enum import Enum
from pathlib import Path

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.operations.create_value import CreateValueModule
from kiara.operations.data_export import DataExportModule
from kiara.operations.extract_metadata import ExtractMetadataModule
from kiara_modules.core.defaults import DEFAULT_DB_CHUNK_SIZE
from kiara_modules.core.metadata_models import KiaraFile
from kiara_modules.core.table.utils import create_sqlite_schema_data_from_arrow_table
from pydantic import BaseModel, Field

from kiara_modules.network_analysis.defaults import (
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_modules.network_analysis.metadata_models import (
    NetworkData,
    NetworkDataSchema,
)
from kiara_modules.network_analysis.utils import insert_table_data_into_network_graph

if typing.TYPE_CHECKING:
    import pyarrow as pa


KIARA_METADATA = {
    "authors": [{"name": "Markus Binsteiner", "email": "markus@frkl.io"}],
}


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
                "default": SOURCE_COLUMN_NAME,
            },
            "edges_target_column_name": {
                "type": "string",
                "doc": "The name of the target column name in the edges table.",
                "default": TARGET_COLUMN_NAME,
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
                "default": ID_COLUMN_NAME,
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
            table=edges_table,
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
                extra_column_info={ID_COLUMN_NAME: ["NOT NULL", "UNIQUE"]},
            )

        else:
            nodes_data = None

        nd_schema = NetworkDataSchema(edges_schema=edges_data, nodes_schema=nodes_data)
        init_sql = nd_schema.create_init_sql()

        network_data = NetworkData.create_in_temp_dir(init_sql=init_sql)
        insert_table_data_into_network_graph(
            network_data=network_data,
            edges_table=edges_table,
            edges_schema=edges_data,
            nodes_table=nodes_table,
            nodes_schema=nodes_data,
            chunk_size=DEFAULT_DB_CHUNK_SIZE,
        )

        outputs.set_value("network_data", network_data)


class ExportNetworkDataModule(DataExportModule):
    @classmethod
    def get_source_value_type(cls) -> str:
        return "network_data"

    def export_as__graphml_file(self, value: NetworkData, base_path: str, name: str):

        import networkx as nx

        target_path = os.path.join(base_path, f"{name}.graphml")

        # TODO: can't just assume digraph
        graph: nx.Graph = value.as_networkx_graph(nx.DiGraph)
        nx.write_graphml(graph, target_path)

        return {"files": target_path}

    def export_as__sqlite_db(self, value: NetworkData, base_path: str, name: str):

        target_path = os.path.abspath(os.path.join(base_path, f"{name}.sqlite"))
        shutil.copy2(value.db_file_path, target_path)

        return {"files": target_path}

    def export_as__sql_dump(self, value: NetworkData, base_path: str, name: str):

        import sqlite_utils

        db = sqlite_utils.Database(value.db_file_path)
        target_path = Path(os.path.join(base_path, f"{name}.sql"))
        with target_path.open("wt") as f:
            for line in db.conn.iterdump():
                f.write(line + "\n")

        return {"files": target_path}

    def export_as__csv_files(self, value: NetworkData, base_path: str, name: str):

        import sqlite3

        files = []

        for table_name in value.table_names:
            target_path = os.path.join(base_path, name, f"{table_name}.csv")
            os.makedirs(os.path.dirname(target_path), exist_ok=True)

            # copied from: https://stackoverflow.com/questions/2952366/dump-csv-from-sqlalchemy
            con = sqlite3.connect(value.db_file_path)
            outfile = open(target_path, "wt")
            outcsv = csv.writer(outfile)

            cursor = con.execute(f"select * from {table_name}")
            # dump column titles (optional)
            outcsv.writerow(x[0] for x in cursor.description)
            # dump rows
            outcsv.writerows(cursor.fetchall())

            outfile.close()
            files.append(target_path)

        return {"files": files}


class CreateNetworkDataModule(CreateValueModule):
    @classmethod
    def get_target_value_type(cls) -> str:
        return "network_data"

    def from_graphml_file(self, value: Value):

        import networkx as nx

        input_file: KiaraFile = value.get_value_data()

        graph = nx.read_graphml(input_file.path)

        network_data = NetworkData.create_from_networkx_graph(graph)
        return network_data

    def from_gexf_file(self, value: Value):

        import networkx as nx

        input_file: KiaraFile = value.get_value_data()

        graph = nx.read_gexf(input_file.path)
        graph = nx.relabel_gexf_graph(graph)

        network_data = NetworkData.create_from_networkx_graph(graph)
        return network_data

    def from_shp_file(self, value: Value):

        import networkx as nx

        input_file: KiaraFile = value.get_value_data()

        graph = nx.read_shp(input_file.path)

        network_data = NetworkData.create_from_networkx_graph(graph)
        return network_data

    def from_gml_file(self, value: Value):

        import networkx as nx

        input_file: KiaraFile = value.get_value_data()

        graph = nx.read_gml(input_file.path)

        network_data = NetworkData.create_from_networkx_graph(graph)
        return network_data


# class NetworkDataTest(KiaraModule):
#
#     _module_type_name = "test"
#
#     def create_input_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#
#         return {
#             "network_data": {
#                 "type": "network_data"
#             }
#         }
#
#     def create_output_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#
#         return {
#             "network_data": {
#                 "type": "network_data"
#             }
#         }
#
#
#     def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
#
#         network_data_value = inputs.get_value_obj("network_data")
#         network_data: NetworkData = network_data_value.get_value_data()
#
#         graph = network_data.as_networkx_graph(nx.DiGraph)
#
#         n2 = NetworkData.create_from_networkx_graph(graph=graph)
#
#         outputs.set_value("network_data", n2)
